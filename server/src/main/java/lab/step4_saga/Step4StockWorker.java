package lab.step4_saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import lab.domain.Orders;
import lab.domain.ProcessedMessage;
import lab.domain.Product;
import lab.domain.OutboxEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class Step4StockWorker {

    private final ObjectMapper objectMapper;
    private final Step4ProductRepository productRepository;
    private final Step4OrderRepository orderRepository;
    private final Step4OutboxRepository outboxRepository;
    private final Step4ProcessedMessageRepository processedMessageRepository;

    /**
     * flash-order-requests 토픽에서 주문 접수 요청을 가져와
     * 로컬 트랜잭션으로 안전하게 재고를 차감하고 PENDING 주문 및 Outbox 이벤트를 생성합니다.
     */
    @KafkaListener(topics = Step4KafkaConfig.TOPIC_FLASH_REQUESTS, groupId = "saga-stock-group")
    @Transactional
    public void processOrderRequest(String messagePayload) {
        try {
            Step4Dtos.FlashOrderRequest request = objectMapper.readValue(messagePayload, Step4Dtos.FlashOrderRequest.class);
            String txId = request.txId();

            // 1. 멱등성 검사 (재고 중복 차감 방지)
            if (!tryMarkProcessed("stock-" + txId)) {
                log.warn("[StockWorker] 중복 요청 감지되어 SKIP: txId={}", txId);
                return;
            }

            Long productId = request.productId() != null ? request.productId() : 1L;
            Long userId = request.userId() != null ? request.userId() : 1L;
            int qty = request.quantity() != null ? request.quantity() : 1;

            // 2. 비관적 락으로 재고 확인 및 차감
            Product product = productRepository.findByIdForUpdate(productId)
                    .orElse(null);

            if (product == null || product.getStock() < qty) {
                log.info("[StockWorker] 재고 부족으로 주문 탈락: productId={}, stock={}", productId, (product == null ? 0 : product.getStock()));
                return;
            }

            product.setStock(product.getStock() - qty);

            // 3. Orders 테이블에 PENDING 상태로 INSERT
            Orders order = Orders.builder()
                    .userId(userId)
                    .productId(productId)
                    .status("PENDING")
                    .txId(txId)
                    .build();
            orderRepository.save(order);

            // 4. 동일 트랜잭션 내에서 결제 트리거용 OutboxEvent(ORDER_CREATED) INSERT
            Step4Dtos.SagaEventPayload outboxPayload = new Step4Dtos.SagaEventPayload(
                    "ORDER_CREATED",
                    order.getId(),
                    userId,
                    productId,
                    qty,
                    txId
            );

            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .aggregateId(txId)
                    .payload(objectMapper.writeValueAsString(outboxPayload))
                    .status("INIT")
                    .createdAt(LocalDateTime.now())
                    .build();
            outboxRepository.save(outboxEvent);

            log.info("[StockWorker] 재고 차감 성공 및 Outbox 생성: orderId={}, remainingStock={}", order.getId(), product.getStock());

        } catch (Exception e) {
            log.error("[StockWorker Error] 처리 실패: {}", messagePayload, e);
            throw new RuntimeException(e);
        }
    }

    private boolean tryMarkProcessed(String key) {
        if (processedMessageRepository.existsById(key)) {
            return false;
        }
        try {
            processedMessageRepository.saveAndFlush(new ProcessedMessage(key, LocalDateTime.now()));
            return true;
        } catch (DataIntegrityViolationException e) {
            return false;
        }
    }
}
