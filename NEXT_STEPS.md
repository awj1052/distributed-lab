# 🚀 다음 단계 로드맵: 분산 시스템 심화 가이드 (NEXT_STEPS.md)

이 문서는 `distributed-lab` (Step 1~4: RDBMS 한계 체감부터 Kafka, Outbox, Saga 패턴까지)을 성공적으로 마친 후, **실무 대규모 엔터프라이즈 시스템 수준으로 확장하기 위해 학습해야 할 다음 4가지 핵심 주제**를 정리한 가이드입니다.

---

## 1. Step 5: Redis 기반 분산 락(Redisson)과 초고속 재고 차감

### 🔍 왜 필요한가?
- 우리 1단계에서는 PostgreSQL 비관적 락(`SELECT ... FOR UPDATE`)을 썼기 때문에 동시 요청 시 DB 커넥션 풀이 마르는 문제를 겪었습니다.
- 4단계에서는 Kafka 버퍼링을 도입했지만, 결국 첫 번째 워커(StockWorker)가 DB에 락을 걸고 재고를 차감합니다.

### 🛠️ 구현해 볼 과제
1. **Redisson 분산 락 도입**:
   - DB 트랜잭션 락 대신 Redis의 `RLock`을 활용하여 분산 환경에서 임계 영역(Critical Section) 보호.
   - `tryLock(waitTime, leaseTime)`을 사용해 무한 대기를 방지하고 락 자동 반납 보장.
2. **Redis Lua Script 원자적 재고 차감 (극한의 최적화)**:
   - DB에 쿼리를 날리지 않고, Redis 인메모리에서 Lua 스크립트로 `재고 확인 -> 0보다 크면 DECR`을 단 1회 네트워크 왕복으로 원자적 실행.
   - 성공한 주문만 Kafka를 통해 DB로 비동기 영속화(배치 INSERT).

---

## 2. Step 6: 장애 격리 — 서킷 브레이커(Resilience4j) & Dead Letter Queue (DLQ)

### 🔍 왜 필요한가?
- 3단계와 4단계에서는 외부 결제사(PG) 실패 시 보상 트랜잭션을 날리거나 Outbox가 재시도(Retry)를 했습니다.
- 하지만 **외부 결제사 서버 자체가 완전히 다운되었을 때** 계속 요청을 보내면 내 서버의 스레드와 리소스까지 연쇄적으로 고갈(Cascading Failure)됩니다.
- 또한 결함이 있는 특정 메시지가 무한 재시도되면 전체 큐의 처리가 멈추는 **Head-of-Line Blocking**이 발생합니다.

### 🛠️ 구현해 볼 과제
1. **Resilience4j 서킷 브레이커 (Circuit Breaker)**:
   - 외부 API 호출 실패율이 50%를 넘으면 서킷을 `OPEN`하여 즉시 빠른 실패(Fail-Fast) 처리.
   - 외부 서버가 복구되면 점진적으로 요청을 흘려보내는 `HALF-OPEN` 상태 전이 실습.
2. **Kafka DLQ (Dead Letter Queue)**:
   - 컨슈머가 3회 이상 처리 실패 시, 해당 메시지를 버리지 않고 `flash-order-dlq`라는 격리 토픽으로 이동.
   - 본래의 정상 큐는 지연 없이 계속 흐르도록 보장하고, 실패한 메시지는 알림 발송 및 운영자가 수동 분석/재처리할 수 있도록 파이프라인 구축.

---

## 3. Step 7: 엔터프라이즈 Outbox — Debezium CDC (Change Data Capture)

### 🔍 왜 필요한가?
- 우리 3단계와 4단계에서는 Spring Boot의 `@Scheduled(fixedDelay = 500)` 스케줄러가 0.5초마다 DB 테이블을 `SELECT ... WHERE status = 'INIT'`으로 계속 폴링했습니다.
- **문제점**:
  1. 이벤트가 많아지면 DB에 인덱스 부하 및 주기적인 I/O 부하가 발생합니다.
  2. 0.5초라는 폴링 딜레이(지연 시간)가 존재합니다.

### 🛠️ 구현해 볼 과제
1. **Debezium + Kafka Connect 연동**:
   - 애플리케이션 코드는 스케줄러를 완전히 제거하고, 단지 DB 트랜잭션 내에서 `outbox_event`에 `INSERT`만 수행.
   - Debezium 엔진이 PostgreSQL의 **트랜잭션 로그(WAL: Write-Ahead Log)**를 실시간 캡처하여 0ms에 가깝게 Kafka 토픽으로 자동 스트리밍.
2. 실무에서 가장 권장되는 **"Zero-Polling Transactional Outbox"** 구조 완성.

---

## 4. Step 8: 분산 트레이싱(Distributed Tracing) & 가시성 (OpenTelemetry)

### 🔍 왜 필요한가?
- 분산 비동기 환경에서는 요청이 하나의 스레드에서 끝나지 않습니다:
  `클라이언트 -> API 컨트롤러 -> Kafka -> StockWorker -> Outbox -> Kafka -> PaymentWorker -> Kafka -> CompensationWorker`
- 로그를 조회하면 수많은 비동기 스레드 로그가 뒤섞여, 특정 주문 하나(`txId`)가 어떤 경로로 왜 실패했는지 추적하기가 매우 어렵습니다.

### 🛠️ 구현해 볼 과제
1. **OpenTelemetry + Micrometer Tracing (Zipkin / Jaeger)**:
   - 최초 HTTP 요청이 들어올 때 고유한 `TraceId` 발급.
   - Kafka 메시지 헤더에 `TraceId`와 `SpanId`를 전파(Propagation)하여 비동기 워커까지 동일한 ID로 추적 유지.
   - Zipkin 또는 Jaeger 대시보드에서 주문 한 건의 전체 흐름과 각 구간별 소요 시간을 워터폴 차트로 시각화.

---

## 🔑 심화 학습 추천 키워드

- `Idempotent Producer (멱등성 프로듀서)`
- `Exactly-Once Semantics (EOS / 정확히 한 번 처리)`
- `Transactional Outbox with Debezium (CDC 기반 아웃박스)`
- `Event Sourcing & CQRS (이벤트 소싱과 명령/조회 분리)`
- `Kafka Partition Rebalance & Consumer Group Lag`
- `Distributed Lock with Redisson (레디스 분산 락)`
