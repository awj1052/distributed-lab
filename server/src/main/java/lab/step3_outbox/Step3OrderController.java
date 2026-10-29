package lab.step3_outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/v3/orders")
@RequiredArgsConstructor
public class Step3OrderController {

    private final Step3OrderService orderService;
    private final Step3OutboxRelayScheduler relayScheduler;
    private final Step3OrderRepository orderRepository;
    private final Step3OutboxRepository outboxRepository;
    private final Step3OrderConsumer orderConsumer;

    /**
     * 엔드포인트 1: Dual-Write (20% Kafka 장애로 인한 데이터 불일치 유발)
     */
    @PostMapping("/dual-write")
    public ResponseEntity<Step3Dtos.OrderResponse> createDualWriteOrder(
            @RequestParam(defaultValue = "0.2") double failRate,
            @RequestBody(required = false) Step3Dtos.OrderRequest request) {
        if (request == null) {
            request = new Step3Dtos.OrderRequest(1L, 1L, 1, null);
        }
        Step3Dtos.OrderResponse response = orderService.createDualWriteOrder(request, failRate);
        return ResponseEntity.ok(response);
    }

    /**
     * 엔드포인트 2: Transactional Outbox 패턴
     */
    @PostMapping("/outbox")
    public ResponseEntity<Step3Dtos.OrderResponse> createOutboxOrder(
            @RequestParam(defaultValue = "0.2") double failRate,
            @RequestBody(required = false) Step3Dtos.OrderRequest request) {
        if (request == null) {
            request = new Step3Dtos.OrderRequest(1L, 1L, 1, null);
        }
        relayScheduler.setRelayFailRate(failRate);
        Step3Dtos.OrderResponse response = orderService.createOutboxOrder(request);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    /**
     * 검증 엔드포인트: DB 주문 건수 vs Outbox 이벤트 vs Consumer 수신 건수 비교
     */
    @GetMapping("/verify")
    public ResponseEntity<Step3Dtos.Step3VerifyResponse> verifyConsistency() {
        long dbOrders = orderRepository.count();
        long outboxInit = outboxRepository.countByStatus("INIT");
        long outboxPublished = outboxRepository.countByStatus("PUBLISHED");
        long consumerReceived = orderConsumer.getConsumedCount().get();

        boolean isConsistent = (dbOrders == consumerReceived) && (outboxInit == 0);

        return ResponseEntity.ok(new Step3Dtos.Step3VerifyResponse(
                dbOrders,
                outboxInit,
                outboxPublished,
                consumerReceived,
                isConsistent
        ));
    }
}
