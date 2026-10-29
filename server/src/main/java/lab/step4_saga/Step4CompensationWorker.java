package lab.step4_saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import lab.domain.Orders;
import lab.domain.ProcessedMessage;
import lab.domain.Product;
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
public class Step4CompensationWorker {

    private final ObjectMapper objectMapper;
    private final Step4ProductRepository productRepository;
    private final Step4OrderRepository orderRepository;
    private final Step4ProcessedMessageRepository processedMessageRepository;

    /**
     * payment-failed-v4 이벤트를 수신하여 보상 트랜잭션(Saga Compensation)을 실행합니다.
     * 차감되었던 재고를 다시 원복(+qty)하고 주문 상태를 CANCELLED로 변경합니다.
     */
    @KafkaListener(topics = Step4KafkaConfig.TOPIC_PAYMENT_FAILED, groupId = "saga-compensation-group")
    @Transactional
    public void processCompensation(String messagePayload) {
        try {
            Step4Dtos.SagaEventPayload event = objectMapper.readValue(messagePayload, Step4Dtos.SagaEventPayload.class);
            String txId = event.txId();

            // 1. 보상 처리 멱등성 검사
            if (!tryMarkProcessed("comp-" + txId)) {
                log.warn("[CompensationWorker] 이미 실행된 보상 트랜잭션이므로 SKIP: txId={}", txId);
                return;
            }

            // 2. 재고 원복 (+qty)
            Long productId = event.productId() != null ? event.productId() : 1L;
            int qty = event.quantity() != null ? event.quantity() : 1;

            Product product = productRepository.findByIdForUpdate(productId).orElse(null);
            if (product != null) {
                product.setStock(product.getStock() + qty);
            }

            // 3. 주문 상태 CANCELLED 확정
            orderRepository.findByTxId(txId).ifPresent(order -> {
                order.setStatus("CANCELLED");
                orderRepository.save(order);
            });

            log.info("[CompensationWorker] 🔄 보상 트랜잭션 완료! 재고 원복(+{}) 및 주문 취소 확정: txId={}, restoredStock={}",
                    qty, txId, (product == null ? 0 : product.getStock()));

        } catch (Exception e) {
            log.error("[CompensationWorker Error] 보상 트랜잭션 실패: {}", messagePayload, e);
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
