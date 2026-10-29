package lab.step4_saga;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lab.domain.Product;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v4")
@RequiredArgsConstructor
public class Step4OrderController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final Step4ProductRepository productRepository;
    private final Step4OrderRepository orderRepository;
    private final Step4OutboxRepository outboxRepository;

    /**
     * 엔드포인트 1: 대규모 선착순 주문 버퍼링 방파제
     * 중요: DB 커넥션을 전혀 맺지 않고, Kafka 토픽으로 즉시 큐잉 후 202 ACCEPTED 반환!
     */
    @PostMapping("/flash-order")
    public ResponseEntity<Step4Dtos.FlashOrderResponse> flashOrder(
            @RequestBody(required = false) Step4Dtos.FlashOrderRequest request) {
        if (request == null) {
            request = new Step4Dtos.FlashOrderRequest(1L, 1L, 1, null);
        }

        String txId = (request.txId() != null && !request.txId().isBlank())
                ? request.txId()
                : "saga-" + UUID.randomUUID();

        Step4Dtos.FlashOrderRequest payload = new Step4Dtos.FlashOrderRequest(
                request.productId() != null ? request.productId() : 1L,
                request.userId() != null ? request.userId() : 1L,
                request.quantity() != null ? request.quantity() : 1,
                txId
        );

        try {
            // DB 통신 0회, 오직 브로커로만 버퍼링
            kafkaTemplate.send(Step4KafkaConfig.TOPIC_FLASH_REQUESTS, txId, objectMapper.writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("직렬화 실패", e);
        }

        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(new Step4Dtos.FlashOrderResponse("ACCEPTED", txId, "선착순 주문 접수 완료 (버퍼링 대기)"));
    }

    /**
     * 엔드포인트 2: Saga 최종 일관성 및 불변식 검증
     * 불변식:
     * 1. 차감된 실재고 (초기재고 1,000 - 현재재고) == 결제 성공 주문(PAID) 건수
     * 2. 결제 실패(CANCELLED) 주문의 재고는 100% 보상 트랜잭션으로 원복됨
     * 3. 모든 비동기 처리 완료 후 PENDING 주문 및 INIT Outbox가 0건이어야 함
     */
    @GetMapping("/verify-saga")
    public ResponseEntity<Step4Dtos.SagaVerifyResponse> verifySaga() {
        int initialStock = 1000;
        Product product = productRepository.findById(1L).orElse(null);
        int currentStock = (product != null) ? product.getStock() : 0;
        int deductedStock = initialStock - currentStock;

        long paidCount = orderRepository.countByStatus("PAID");
        long cancelledCount = orderRepository.countByStatus("CANCELLED");
        long pendingCount = orderRepository.countByStatus("PENDING");
        long outboxInitCount = outboxRepository.countByStatus("INIT");

        boolean isStockAndPaidConsistent = (deductedStock == paidCount);
        boolean isOutboxDrained = (outboxInitCount == 0 && pendingCount == 0);
        boolean isConsistent = isStockAndPaidConsistent && isOutboxDrained;

        return ResponseEntity.ok(new Step4Dtos.SagaVerifyResponse(
                initialStock,
                currentStock,
                deductedStock,
                paidCount,
                cancelledCount,
                pendingCount,
                outboxInitCount,
                isStockAndPaidConsistent,
                isOutboxDrained,
                isConsistent
        ));
    }
}
