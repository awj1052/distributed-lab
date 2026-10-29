#!/usr/bin/env node

/**
 * bench/bench.mjs
 * 분산 시스템 실습용 단일 스크립트 공용 부하 테스트 & 벤치마크 분석 도구
 * 
 * - 외부 라이브러리(npm install) 없이 Node.js 내장 fetch API만으로 구동
 * - 동시성 제어 부하 전송 (Worker Pool 패턴)
 * - 50ms 주기 백그라운드 메트릭 폴링 (HikariCP Active / Pending Threads)
 * - PostgreSQL 데드락(40P01) 및 HikariCP 타임아웃 에러 패턴 분석
 * - bench/results.md 자동 Markdown 테이블 리포팅
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const RESULTS_FILE = path.join(__dirname, 'results.md');

// 시나리오 정의 목록
const SCENARIO_DEFS = {
  'step1-hot-lock': {
    name: '1-A. 비관적 락 (Hot Row)',
    path: '/api/v1/orders/hot-lock',
    method: 'POST',
    makeBody: (idx) => ({ productId: 1, userId: 1, quantity: 1, reqId: idx })
  },
  'step1-delay': {
    name: '1-B. 외부 API 지연 (Pool 고갈)',
    path: '/api/v1/orders/external-delay',
    method: 'POST',
    makeBody: (idx) => ({ productId: 1, userId: 1, quantity: 1, reqId: idx })
  },
  'step1-deadlock': {
    name: '1-C. 교차 락 (Deadlock 발생)',
    path: '/api/v1/orders/deadlock',
    method: 'POST',
    // 짝수는 1->2 순서 요청, 홀수는 2->1 순서 요청으로 교차 잠금 유도
    makeBody: (idx) => (idx % 2 === 0 
      ? { firstProductId: 1, secondProductId: 2, userId: (idx % 10) + 1 }
      : { firstProductId: 2, secondProductId: 1, userId: (idx % 10) + 1 }
    )
  },
  'step1-deadlock-fixed': {
    name: '1-D. 락 순서 정렬 (Deadlock 해결)',
    path: '/api/v1/orders/deadlock-fixed',
    method: 'POST',
    makeBody: (idx) => (idx % 2 === 0 
      ? { firstProductId: 1, secondProductId: 2, userId: (idx % 10) + 1 }
      : { firstProductId: 2, secondProductId: 1, userId: (idx % 10) + 1 }
    )
  },
  'step2-async': {
    name: '2-A. 비동기 큐 분리 (Kafka)',
    path: '/api/v2/orders/async',
    method: 'POST',
    makeBody: (idx) => ({ productId: 1, userId: 1, quantity: 1, txId: `tx-v2-${idx}` })
  },
  'step2-dup-no-idempotent': {
    name: '2-B. 중복 메시지 (멱등성 미적용)',
    path: '/api/v2/orders/duplicate-test?idempotent=false',
    method: 'POST',
    makeBody: (idx) => ({ productId: 1, userId: 1, quantity: 1, txId: `dup-${Math.floor(idx / 2)}` })
  },
  'step2-dup-idempotent': {
    name: '2-C. 중복 메시지 (멱등성 적용)',
    path: '/api/v2/orders/duplicate-test?idempotent=true',
    method: 'POST',
    makeBody: (idx) => ({ productId: 1, userId: 1, quantity: 1, txId: `dup-${Math.floor(idx / 2)}` })
  },
  'step3-dual-write': {
    name: '3-A. Dual-Write (20% 장애)',
    path: '/api/v3/orders/dual-write?failRate=0.2',
    method: 'POST',
    makeBody: (idx) => ({ productId: 1, userId: 1, quantity: 1, txId: `dw-${idx}` })
  },
  'step3-outbox': {
    name: '3-B. Transactional Outbox',
    path: '/api/v3/orders/outbox?failRate=0.2',
    method: 'POST',
    makeBody: (idx) => ({ productId: 1, userId: 1, quantity: 1, txId: `ob-${idx}` })
  },
  'step4-saga': {
    name: '4. 버퍼링 + Saga 보상 트랜잭션',
    path: '/api/v4/flash-order',
    method: 'POST',
    makeBody: (idx) => ({ productId: 1, userId: (idx % 20) + 1, quantity: 1, txId: `saga-${idx}` })
  }
};

// CLI 인자 파싱
function parseArgs() {
  const args = process.argv.slice(2);
  const config = {
    scenario: 'step1-hot-lock',
    concurrency: 100,
    requests: 500,
    all: false,
    baseUrl: 'http://localhost:8080',
    help: false
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--scenario' && args[i + 1]) {
      config.scenario = args[++i];
    } else if (arg === '--concurrency' && args[i + 1]) {
      config.concurrency = parseInt(args[++i], 10);
    } else if (arg === '--requests' && args[i + 1]) {
      config.requests = parseInt(args[++i], 10);
    } else if (arg === '--target' && args[i + 1]) {
      config.baseUrl = args[++i].replace(/\/$/, '');
    } else if (arg === '--all') {
      config.all = true;
    } else if (arg === '--help' || arg === '-h') {
      config.help = true;
    }
  }

  return config;
}

// 초기화 요청 (/admin/reset)
async function triggerReset(baseUrl) {
  try {
    const res = await fetch(`${baseUrl}/admin/reset`, { method: 'POST' });
    if (res.ok) {
      console.log(`[Admin] DB Reset 성공 (${baseUrl}/admin/reset)`);
    } else {
      console.log(`[Admin] DB Reset 응답 코드: ${res.status} (아직 엔드포인트 미구현 상태일 수 있음)`);
    }
  } catch (err) {
    console.log(`[Admin] DB Reset 스킵: 서버 미응답 (${err.message})`);
  }
}

// 50ms 실시간 HikariCP 메트릭 폴러
class MetricsPoller {
  constructor(baseUrl, intervalMs = 50) {
    this.baseUrl = baseUrl;
    this.intervalMs = intervalMs;
    this.peakActiveConnections = 0;
    this.peakThreadsAwaiting = 0;
    this.running = false;
    this.timer = null;
  }

  start() {
    this.running = true;
    this.timer = setInterval(async () => {
      if (!this.running) return;
      try {
        const res = await fetch(`${this.baseUrl}/admin/metrics`, {
          signal: AbortSignal.timeout(300)
        });
        if (res.ok) {
          const data = await res.json();
          const active = data.activeConnections ?? 0;
          const awaiting = data.threadsAwaitingConnection ?? 0;
          if (active > this.peakActiveConnections) this.peakActiveConnections = active;
          if (awaiting > this.peakThreadsAwaiting) this.peakThreadsAwaiting = awaiting;
        }
      } catch {
        // 부하 중 타임아웃 또는 서버 미구현 시 무시
      }
    }, this.intervalMs);
  }

  stop() {
    this.running = false;
    if (this.timer) clearInterval(this.timer);
  }
}

// 단일 HTTP 요청 실행 및 에러 분류
async function sendRequest(url, method, body) {
  const start = performance.now();
  let status = 0;
  let isTimeout = false;
  let isDeadlock = false;
  let isSuccess = false;
  let errorText = '';

  try {
    const res = await fetch(url, {
      method,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(10000) // 클라이언트 10초 타임아웃
    });

    status = res.status;
    const text = await res.text();

    if (res.ok || status === 202) {
      isSuccess = true;
    } else {
      errorText = text;
      // HikariCP 커넥션 풀 타임아웃 감지
      if (
        text.includes('SQLTransientConnectionException') ||
        text.includes('Connection is not available, request timed out') ||
        text.includes('timed out')
      ) {
        isTimeout = true;
      }
      // PostgreSQL 데드락 감지 (SQLState 40P01 또는 deadlock detected)
      if (
        text.includes('40P01') ||
        text.toLowerCase().includes('deadlock') ||
        text.includes('deadlock_detected')
      ) {
        isDeadlock = true;
      }
    }
  } catch (err) {
    status = -1;
    errorText = err.message;
    if (err.message.includes('timeout') || err.name === 'TimeoutError') {
      isTimeout = true;
    }
  }

  const duration = performance.now() - start;
  return { status, duration, isSuccess, isTimeout, isDeadlock, errorText };
}

// 시나리오 부하 실행 함수
async function runScenario(scenarioKey, config) {
  const scenario = SCENARIO_DEFS[scenarioKey];
  if (!scenario) {
    console.error(`존재하지 않는 시나리오: ${scenarioKey}`);
    return null;
  }

  console.log(`\n======================================================`);
  console.log(`🚀 [시나리오 시작] ${scenario.name} (${scenarioKey})`);
  console.log(`- Target: ${config.baseUrl}${scenario.path}`);
  console.log(`- Concurrency: ${config.concurrency}, Total Requests: ${config.requests}`);
  console.log(`======================================================`);

  // 1. 테스트 전 DB 리셋 시도
  await triggerReset(config.baseUrl);

  // 2. 백그라운드 메트릭 폴러 가동
  const poller = new MetricsPoller(config.baseUrl, 50);
  poller.start();

  // 3. Worker Pool 패턴 부하 전송
  let currentIndex = 0;
  const latencies = [];
  let successCount = 0;
  let timeoutCount = 0;
  let deadlockCount = 0;
  let notFoundCount = 0; // 404 카운트
  let otherErrorCount = 0;

  const startTime = performance.now();

  const worker = async () => {
    while (true) {
      const idx = currentIndex++;
      if (idx >= config.requests) break;

      const body = scenario.makeBody(idx);
      const url = `${config.baseUrl}${scenario.path}`;
      const result = await sendRequest(url, scenario.method, body);

      latencies.push(result.duration);
      if (result.isSuccess) {
        successCount++;
      } else {
        if (result.status === 404) notFoundCount++;
        if (result.isTimeout) timeoutCount++;
        if (result.isDeadlock) deadlockCount++;
        if (result.status !== 404 && !result.isTimeout && !result.isDeadlock) {
          otherErrorCount++;
        }
      }
    }
  };

  const workers = Array.from({ length: config.concurrency }, () => worker());
  await Promise.all(workers);

  const totalDuration = (performance.now() - startTime) / 1000; // 초 단위
  poller.stop();

  // 통계 계산
  latencies.sort((a, b) => a - b);
  const p50 = latencies[Math.floor(latencies.length * 0.5)] || 0;
  const p95 = latencies[Math.floor(latencies.length * 0.95)] || 0;
  const p99 = latencies[Math.floor(latencies.length * 0.99)] || 0;
  const tps = (config.requests / totalDuration).toFixed(1);

  const summary = {
    scenarioKey,
    name: scenario.name,
    requests: config.requests,
    concurrency: config.concurrency,
    durationSec: totalDuration.toFixed(2),
    tps,
    p50: p50.toFixed(1),
    p95: p95.toFixed(1),
    p99: p99.toFixed(1),
    successCount,
    timeoutCount,
    deadlockCount,
    notFoundCount,
    otherErrorCount,
    peakActiveConnections: poller.peakActiveConnections,
    peakThreadsAwaiting: poller.peakThreadsAwaiting
  };

  console.log(`\n📊 [결과 요약]`);
  console.log(`- 실행 시간: ${summary.durationSec}s | 처리량: ${summary.tps} TPS`);
  console.log(`- 응답 지연: P50=${summary.p50}ms | P95=${summary.p95}ms | P99=${summary.p99}ms`);
  console.log(`- 커넥션 풀 최대 대기(Pending): ${summary.peakThreadsAwaiting} 스레드 (최대 활성: ${summary.peakActiveConnections})`);
  console.log(`- 에러 집계: 성공=${summary.successCount}, 타임아웃=${summary.timeoutCount}, 데드락=${summary.deadlockCount}, 404(미구현)=${summary.notFoundCount}`);

  await appendResultToMarkdown(summary);
  return summary;
}

// Markdown 테이블 결과 기록 (bench/results.md)
async function appendResultToMarkdown(s) {
  let fileContent = '';
  try {
    fileContent = await fs.readFile(RESULTS_FILE, 'utf-8');
  } catch {
    // 파일 없으면 새로 생성
  }

  const header = 
`# 벤치마크 테스트 결과 리포트

| 실행일시 | 시나리오 | Concurrency / Req | TPS | P95 지연 | DB 최대 대기 스레드 | 커넥션 타임아웃 | 데드락 | 성공 / 404 / 기타에러 |
| :--- | :--- | :---: | ---: | ---: | ---: | ---: | ---: | :---: |
`;

  if (!fileContent.includes('| 실행일시 | 시나리오 |')) {
    fileContent = header;
  }

  const now = new Date().toISOString().replace('T', ' ').substring(0, 19);
  const row = `| ${now} | **${s.name}** | ${s.concurrency} / ${s.requests} | ${s.tps} | ${s.p95}ms | ${s.peakThreadsAwaiting} | ${s.timeoutCount}건 | ${s.deadlockCount}건 | ${s.successCount} / ${s.notFoundCount} / ${s.otherErrorCount} |\n`;

  await fs.writeFile(RESULTS_FILE, fileContent + row, 'utf-8');
  console.log(`📝 [리포트 저장 완료] ${RESULTS_FILE}`);
}

// 도움말 출력
function showHelp() {
  console.log(`
사용법: node bench/bench.mjs [옵션]

옵션 목록:
  --scenario <name>     실행할 시나리오 (기본값: step1-hot-lock)
  --concurrency <num>   동시 요청 수 (기본값: 100)
  --requests <num>      총 요청 수 (기본값: 500)
  --target <url>        서버 Base URL (기본값: http://localhost:8080)
  --all                 등록된 모든 시나리오 순차 실행
  --help, -h            도움말 출력

지원 시나리오 목록:
  step1-hot-lock          1-A. 비관적 락 (Hot Row 경합)
  step1-delay             1-B. 외부 API 지연 (커넥션 풀 고갈)
  step1-deadlock          1-C. 교차 락 (데드락 발생)
  step1-deadlock-fixed    1-D. 락 순서 정렬 (데드락 해결)
  step2-async             2-A. 비동기 큐 분리 (Kafka)
  step2-dup-no-idempotent 2-B. 중복 메시지 (멱등성 미적용)
  step2-dup-idempotent    2-C. 중복 메시지 (멱등성 적용)
  step3-dual-write        3-A. Dual-Write (20% 유실 장애)
  step3-outbox            3-B. Transactional Outbox
  step4-saga              4. 버퍼링 + Saga 보상 트랜잭션
`);
}

// 엔트리포인트 메인 함수
async function main() {
  const config = parseArgs();

  if (config.help) {
    showHelp();
    return;
  }

  if (config.all) {
    console.log(`🎯 [전체 시나리오 순차 실행 모드] 총 ${Object.keys(SCENARIO_DEFS).length}개 시나리오`);
    for (const key of Object.keys(SCENARIO_DEFS)) {
      await runScenario(key, config);
    }
  } else {
    await runScenario(config.scenario, config);
  }
}

main().catch(err => {
  console.error('치명적 에러 발생:', err);
  process.exit(1);
});
