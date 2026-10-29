# 분산 시스템 코드 리딩 & 실습 가이드 (STUDY_GUIDE.md)

이 문서는 [PURPOSE.md](PURPOSE.md)의 4단계 질문들을 **우리 프로젝트의 어느 파일, 몇 번째 줄 코드에서 확인해야 하는지**, 그리고 **직접 코드를 어떻게 고쳐보며(망가뜨려보며) 내 것으로 만들 수 있는지** 정리한 학습 가이드입니다.

> [!TIP]
> **가장 효과적인 공부 순서 (단계당 15분)**
> 1. 아래 안내된 **핵심 코드 파일 링크를 클릭**해 딱 30~50줄만 읽습니다.
> 2. **터미널 명령어**를 복사해 실행하고, 콘솔/DB 결과를 눈으로 확인합니다.
> 3. 각 단계 마지막에 있는 **[직접 해보기 미션]**대로 코드를 1~2줄 수정해서 결과가 어떻게 바뀌는지 확인합니다.

---

## 0. 실습 환경 켜기 (처음 한 번만)

터미널 2개를 띄워두고 시작하면 가장 편합니다.

- **터미널 1 (인프라 & 서버 실행)**:
  ```powershell
  docker compose up -d
  gradle :server:bootRun
  ```
- **터미널 2 (부하 실행 & 검증 명령어 입력용)**:
  ```powershell
  # 서버가 살아있는지 확인
  Invoke-RestMethod -Uri "http://localhost:8080/admin/metrics"
  ```

---

## 1단계: RDBMS의 "물리적 한계"를 명확히 체감하기

> **PURPOSE.md의 핵심 질문**:  
> *"단일 RDBMS에서 `@Transactional`로 모든 것을 해결하던 방식이 어디서 깨지는가?"*

### ① 먼저 봐야 할 설정 파일
- [server/src/main/resources/application.yml](server/src/main/resources/application.yml)
  - `maximum-pool-size: 10`: 서버가 DB와 동시에 맺을 수 있는 연결(커넥션)을 딱 **10개**로 제한했습니다.
  - `connection-timeout: 1000`: 커넥션 10개가 다 쓰이고 있을 때, 11번째 요청은 **1초(1,000ms)** 동안 기다리다 자리가 안 나면 에러(`SQLTransientConnectionException`)를 던지고 죽습니다.

### ② 코드 리딩 포인트: [server/src/main/java/lab/step1_rdbms/Step1OrderService.java](server/src/main/java/lab/step1_rdbms/Step1OrderService.java)

1. **비관적 락(`SELECT ... FOR UPDATE`)이 걸리는 지점**
   - [ProductRepository.java](server/src/main/java/lab/step1_rdbms/ProductRepository.java)의 `@Lock(LockModeType.PESSIMISTIC_WRITE)`를 보세요.
   - `orderWithHotLock()`가 호출되면, 1번 상품 행(Row)에 자물쇠를 채웁니다. 트랜잭션이 끝날(`return`될) 때까지 다른 스레드들은 1번 상품을 건드리지 못하고 줄을 섭니다.

2. **트랜잭션 내부 외부 API 호출 참사 (가장 중요!)**
   - `orderWithExternalDelay()`를 보세요.
   - `Thread.sleep(300)`은 카드사 결제나 카카오톡 알림 발송에 걸리는 **0.3초의 네트워크 대기 시간**을 뜻합니다.
   - **왜 서버가 죽을까요?**: 메서드 위에 `@Transactional`이 붙어 있기 때문에, **DB 작업은 0.001초 만에 끝났는데도 0.3초 동안 DB 커넥션을 안 돌려주고 쥐고 있습니다.**
   - 커넥션이 10개뿐이니 1초에 최대 약 30건(`10개 / 0.3초`)밖에 못 처리합니다. 50명이 동시에 몰리면 앞선 30명만 처리되는 동안 뒤의 20명은 1초 타임아웃에 걸려 전멸합니다!

3. **교차 락 데드락(Deadlock) 발생과 해결**
   - **데드락 발생 코드**: `orderWithDeadlock()`
     - 50% 확률로 어떤 스레드는 `1번 상품 -> 2번 상품` 순서로, 다른 스레드는 `2번 상품 -> 1번 상품` 순서로 락을 잡습니다.
     - 스레드 A가 1번을 쥐고 2번을 기다리는 동시에, 스레드 B가 2번을 쥐고 1번을 기다리면 영원히 안 풀리는 **데드락(SQLState `40P01`)**이 터집니다.
   - **데드락 해결 코드**: `orderWithDeadlockFixed()`
     - `Math.min`, `Math.max`를 보세요. **어떤 요청이 오든 무조건 작은 번호(1번)부터 락을 잡도록 순서를 통일**했습니다. 단 두 줄 바꿨을 뿐인데 데드락이 0건이 됩니다.

### ③ 실행해서 눈으로 확인하기
```powershell
# 1-B. 외부 API 지연으로 커넥션 풀 고갈시키기 (타임아웃 대량 발생 확인)
node bench/bench.mjs --scenario step1-delay --concurrency 50 --requests 150

# 1-C vs 1-D. 데드락 발생(50건) vs 데드락 해결(0건) 비교하기
node bench/bench.mjs --scenario step1-deadlock --concurrency 20 --requests 100
node bench/bench.mjs --scenario step1-deadlock-fixed --concurrency 20 --requests 100
```

### 🧪 [1단계 직접 해보기 미션]
- [Step1OrderService.java](server/src/main/java/lab/step1_rdbms/Step1OrderService.java)의 `Thread.sleep(300)`을 `@Transactional` 메서드 밖(예: [Step1OrderController.java](server/src/main/java/lab/step1_rdbms/Step1OrderController.java)에서 서비스 호출 직후)으로 옮기면 타임아웃 건수가 어떻게 바뀔까요?  
  *(힌트: DB 커넥션을 반납한 뒤에 300ms를 쉬게 되므로 커넥션 풀 타임아웃이 마법처럼 사라집니다!)*

---

## 2단계: "비동기 분리"와 메시지 큐 (Kafka)

> **PURPOSE.md의 핵심 질문**:  
> *"지금 당장 해야 할 일과 나중에 해도 되는 일을 어떻게 나누는가? 같은 메시지가 2번 처리되면 결제가 2번 되나(멱등성)?"*

### ① 코드 리딩 포인트: `lab.step2_async` 패키지

1. **1단계(`v1`)와 2단계(`v2`)의 결정적 차이 비교**
   - **요청 받는 곳**: [Step2OrderService.java](server/src/main/java/lab/step2_async/Step2OrderService.java)
     - 여기서는 DB에 `PENDING` 상태로 `INSERT`만 딱 한 줄 실행하고 바로 메서드가 끝나서 **DB 커넥션을 0.005초 만에 반납**합니다. 그리고 Kafka 토픽(`order-created-v2`)에 쪽지(이벤트)만 던져둡니다.
   - **실제 무거운 일을 하는 곳**: [Step2OrderConsumer.java](server/src/main/java/lab/step2_async/Step2OrderConsumer.java)
     - **`consume` 메서드 선언부를 자세히 보세요! `@Transactional`이 붙어있지 않습니다!**
     - 즉, `Thread.sleep(300)`(외부 API 호출)을 하는 동안 **DB 커넥션을 전혀 쓰지 않습니다.**
     - 300ms 대기가 끝난 뒤, `updateOrderStatusToPaid()`를 호출할 때만 잠깐 커넥션을 빌려 `PAID`로 바꾸고 즉시 반납합니다.

2. **멱등성(Idempotency) 설계 코드 확인**
   - 네트워크가 불안정하면 Kafka 메시지는 **언제든 똑같은 게 2번 이상 배달(At-Least-Once)**될 수 있습니다.
   - `Step2OrderService.createDuplicateOrder()`를 보면 일부러 똑같은 주문 이벤트를 Kafka에 **2번 연속 발행**합니다.
   - 이를 막아주는 방패가 `Step2OrderConsumer.tryRecordProcessedMessage()`입니다.
     - 처리하기 전에 `processed_message` 테이블에 `message_id = txId`를 `INSERT` 시도합니다.
     - 두 번째 중복 메시지가 들어오면 DB의 기본키(PK) 중복 에러(`DataIntegrityViolationException`)가 터지면서 `false`를 반환하고, `"이미 처리된 메시지이므로 SKIP"`하고 빠져나갑니다.

### ② 실행해서 눈으로 확인하기
```powershell
# 1-B와 똑같이 300ms 지연이 있지만, 타임아웃 0건 & 빠른 응답 확인
node bench/bench.mjs --scenario step2-async --concurrency 50 --requests 200

# 중복 메시지 발생 시 멱등성 방어 확인
node bench/bench.mjs --scenario step2-dup-idempotent --concurrency 20 --requests 50
```

### 🧪 [2단계 직접 해보기 미션]
- DB에 접속해서 `processed_message` 테이블에 실제로 키들이 쌓여 있는지 직접 조회해 보세요:
  ```powershell
  docker exec lab-postgres psql -U postgres -d distributed_lab -c "SELECT * FROM processed_message LIMIT 10;"
  ```

---

## 3단계: 분산 환경에서의 데이터 정합성 (Dual-Write vs Outbox)

> **PURPOSE.md의 핵심 질문**:  
> *"DB는 커밋됐는데 Kafka 발행이 실패하면 어떻게 되는가(이중 쓰기 문제)? 이를 Transactional Outbox 패턴으로 어떻게 해결하는가?"*

### ① 코드 리딩 포인트: `lab.step3_outbox` 패키지

1. **왜 단순히 `DB 저장 후 Kafka 전송`을 하면 안 되는가? (Dual-Write 문제)**
   - [Step3OrderService.java](server/src/main/java/lab/step3_outbox/Step3OrderService.java)의 `createDualWriteOrder()`를 보세요.
   - `saveOrderRecord()`로 **DB에는 주문이 이미 저장(커밋)**되었습니다.
   - 그런데 Kafka로 메시지를 보내려다가 20% 확률로 에러(`RuntimeException`)가 납니다.
   - 결과: **DB에는 주문이 100개 있는데, Kafka 컨슈머는 80개밖에 못 받습니다.** 나머지 20개 주문은 영원히 배송/결제 처리가 안 되는 **유령 주문(데이터 유실)**이 됩니다!

2. **해결책: Transactional Outbox 패턴 (2단계로 나눠서 보내기)**
   - **1단계 (원자적 로컬 저장)**: [Step3OrderService.java](server/src/main/java/lab/step3_outbox/Step3OrderService.java)의 `createOutboxOrder()`
     - 메서드 전체가 하나의 `@Transactional`로 묶여 있습니다.
     - Kafka를 직접 호출하는 대신, **같은 PostgreSQL DB 안에 있는 `outbox_event` 테이블에 보낼 메시지(`status = "INIT"`)를 주문(`orders`)과 함께 INSERT** 합니다.
     - 같은 DB 안의 트랜잭션이므로 **둘 다 성공하거나 둘 다 롤백(100% 원자성)**됩니다.
   - **2단계 (백그라운드 릴레이 & 재시도)**: [Step3OutboxRelayScheduler.java](server/src/main/java/lab/step3_outbox/Step3OutboxRelayScheduler.java)
     - 0.5초마다 돌아가는 스케줄러가 `outbox_event` 테이블에서 `INIT` 상태인 행들을 읽어 Kafka로 발송합니다.
     - 20% 확률로 Kafka 장애가 나더라도, **상태가 여전히 `INIT`으로 남아있기 때문에 0.5초 뒤에 다시 시도(Retry)**합니다. 결국 100% 전송이 보장됩니다!

### ② 실행해서 눈으로 확인하기
```powershell
# 1. Dual-Write 실행 후 정합성 깨짐(isConsistent: False) 확인
node bench/bench.mjs --scenario step3-dual-write --concurrency 20 --requests 100
Invoke-RestMethod -Uri "http://localhost:8080/api/v3/orders/verify"

# 2. Outbox 패턴 실행 후 2초 뒤 100% 일치(isConsistent: True) 확인
node bench/bench.mjs --scenario step3-outbox --concurrency 20 --requests 100
Start-Sleep -Seconds 2
Invoke-RestMethod -Uri "http://localhost:8080/api/v3/orders/verify"
```

### 🧪 [3단계 직접 해보기 미션]
- Outbox 테스트를 실행한 직후 `Start-Sleep` 없이 `Invoke-RestMethod -Uri "http://localhost:8080/api/v3/orders/verify"`를 연타해 보세요!  
  `outboxInitCount`가 `100 -> 25 -> 5 -> 0`으로 줄어들면서 `consumerReceivedCount`가 `100`으로 수렴하는 **최종 일관성(Eventual Consistency)** 과정을 실시간으로 볼 수 있습니다.

---

## 4단계: 종합 실습 — 선착순 주문 버퍼링 & Saga 보상 트랜잭션

> **PURPOSE.md의 핵심 질문**:  
> *"초당 1,000건 이상의 폭주하는 요청을 어떻게 버퍼링하고, 분산 환경에서 결제가 실패했을 때 이미 차감한 재고를 어떻게 역방향 롤백(Saga 보상 트랜잭션)하는가?"*

### ① 코드 리딩 포인트: `lab.step4_saga` 패키지 (4개의 클래스만 순서대로 따라가 보세요)

1. **방파제 (API 접수처)**: [Step4OrderController.java](server/src/main/java/lab/step4_saga/Step4OrderController.java)의 `flashOrder()`
   - 이 컨트롤러에는 Repository(DB) 호출이 아예 없습니다!
   - 요청이 오면 Kafka(`flash-order-requests`)에 던지기만 하고 끝납니다. 그래서 HikariCP 커넥션 풀이 단 10개뿐인데도 **약 1,000 TPS, 대기 스레드 0개**로 모든 요청을 튕겨내지 않고 받아냅니다.

2. **첫 번째 워커 (재고 차감)**: [Step4StockWorker.java](server/src/main/java/lab/step4_saga/Step4StockWorker.java)
   - 큐에서 요청을 하나씩 꺼내 재고를 `-1` 차감하고, 주문을 `PENDING`으로 만든 뒤, Outbox에 `"ORDER_CREATED"` 이벤트를 저장합니다.

3. **두 번째 워커 (결제 시도 & 실패 시 보상 이벤트 발행)**: [Step4PaymentWorker.java](server/src/main/java/lab/step4_saga/Step4PaymentWorker.java)
   - **30% 확률(`Math.random() < 0.3`)로 결제 실패**가 일어납니다.
   - 이미 `Step4StockWorker`에서 재고 차감 트랜잭션은 커밋되어 끝난 상태입니다! 과거의 단일 DB처럼 `rollback` 명령어로 되돌릴 수가 없습니다.
   - 그래서 `recordCompensationOutboxEvent()`를 호출해 **"결제가 실패했으니 앞선 재고 차감을 취소해라!"라는 `"PAYMENT_FAILED"` 보상 이벤트**를 발행합니다.

4. **세 번째 워커 (보상 트랜잭션 / 역방향 롤백)**: [Step4CompensationWorker.java](server/src/main/java/lab/step4_saga/Step4CompensationWorker.java)
   - `"PAYMENT_FAILED"` 이벤트를 받아서 **재고를 다시 `+1` 원복(`product.getStock() + qty`)**시키고, **주문 상태를 `"CANCELLED"`로 바꿉니다.**

### ② 실행해서 눈으로 확인하기
```powershell
# 100건 선착순 주문 폭주 인가
node bench/bench.mjs --scenario step4-saga --concurrency 30 --requests 100

# 3초 대기 후 최종 정합성(재고 차감 수 == PAID 주문 수) 검증
Start-Sleep -Seconds 3
Invoke-RestMethod -Uri "http://localhost:8080/api/v4/verify-saga"
```

### 🧪 [4단계 직접 해보기 미션 (강력 추천!)]
1. [Step4CompensationWorker.java](server/src/main/java/lab/step4_saga/Step4CompensationWorker.java)의 재고 복구 코드(`product.setStock(product.getStock() + qty);`)를 **주석 처리(`//`)** 해보세요 (보상 트랜잭션이 고장 난 상황).
2. 서버를 재시작하고 위 명령어를 다시 실행한 뒤 `/api/v4/verify-saga` 결과를 확인해 보세요.
   - 결제는 70건만 성공(`paidOrdersCount: 70`)했는데 재고는 100개가 다 빠져버려서(`deductedStock: 100`), **`isStockAndPaidConsistent: False` (정합성 파괴!)**가 뜨는 것을 직접 눈으로 확인할 수 있습니다!

---

## 5. 한눈에 보는 코드 지도 (Quick Navigation)

| 단계 | 학습 주제 | 핵심 파일 | 벤치마크 실행 명령어 |
| :---: | :--- | :--- | :--- |
| **공통** | DB 커넥션 풀 제한 (Max 10) | [server/src/main/resources/application.yml](server/src/main/resources/application.yml) | - |
| **공통** | 부하 생성 & 메트릭 폴링 | [bench/bench.mjs](bench/bench.mjs) | `node bench/bench.mjs --help` |
| **1단계** | 비관적 락 / 풀 고갈 / 데드락 | [Step1OrderService.java](server/src/main/java/lab/step1_rdbms/Step1OrderService.java) | `node bench/bench.mjs --scenario step1-delay` |
| **2단계** | Kafka 비동기 분리 & 멱등성 | [Step2OrderConsumer.java](server/src/main/java/lab/step2_async/Step2OrderConsumer.java) | `node bench/bench.mjs --scenario step2-async` |
| **3단계** | Dual-Write vs Outbox 패턴 | [Step3OrderService.java](server/src/main/java/lab/step3_outbox/Step3OrderService.java)<br>[Step3OutboxRelayScheduler.java](server/src/main/java/lab/step3_outbox/Step3OutboxRelayScheduler.java) | `node bench/bench.mjs --scenario step3-outbox` |
| **4단계** | 선착순 버퍼링 & Saga 보상 | [Step4OrderController.java](server/src/main/java/lab/step4_saga/Step4OrderController.java)<br>[Step4StockWorker.java](server/src/main/java/lab/step4_saga/Step4StockWorker.java)<br>[Step4PaymentWorker.java](server/src/main/java/lab/step4_saga/Step4PaymentWorker.java)<br>[Step4CompensationWorker.java](server/src/main/java/lab/step4_saga/Step4CompensationWorker.java) | `node bench/bench.mjs --scenario step4-saga` |
