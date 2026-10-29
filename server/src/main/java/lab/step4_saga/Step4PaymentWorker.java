package lab.step4_saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import lab.domain.Orders;
import lab.domain.ProcessedMessage;
import lab.domain.UserPoint;
import lab.domain.OutboxEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class Step4PaymentWorker {

    private final ObjectMapper objectMapper;
    private final Step4OrderRepository orderRepository;
    private final Step4UserPointRepository userPointRepository;
    private final Step4OutboxRepository outboxRepository;
    private final Step4ProcessedMessageRepository processedMessageRepository;

    /**
     * order-created-v4 이벤트를 수신하여 포인트 결제를 시도합니다.
     * 30% 확률로 결제 PG사 실패를 모의하여 보상 트랜잭션(payment-failed-v4)을 Outbox로 발행합니다.
     */
    @KafkaListener(topics = Step4KafkaConfig.TOPIC_ORDER_CREATED, groupId = "saga-payment-group")
    public void processPayment(String messagePayload) {
        try {
            Step4Dtos.SagaEventPayload event = objectMapper.readValue(messagePayload, Step4Dtos.SagaEventPayload.class);
            String txId = event.txId();

            // 1. 결제 처리 멱등성 검사
            if (!tryMarkProcessed("pay-" + txId)) {
                log.warn("[PaymentWorker] 이미 처리된 결제 요청이므로 SKIP: txId={}", txId);
                return;
            }

            // 2. 가상의 결제 PG사 연동 오류 (30% 확률) 시뮬레이션
            boolean paymentFailed = Math.random() < 0.3;

            if (paymentFailed) {
                log.warn("[PaymentWorker] ⚠️ PG 결제 실패 발생 (30% 모의)! Saga 보상 트랜잭션 트리거: txId={}", txId);
                // 보상 트랜잭션 트리거용 Outbox 이벤트(PAYMENT_FAILED) INSERT
                recordCompensationOutboxEvent(event);
            } else {
                // 결제 성공 처리
                completePaymentSuccess(event);
            }

        } catch (Exception e) {
            log.error("[PaymentWorker Error] 결제 처리 실패: {}", messagePayload, e);
        }
    }

    @Transactional
    public void completePaymentSuccess(Step4Dtos.SagaEventPayload event) {
        // 포인트 차감
        Long userId = event.userId() != null ? event.userId() : 1L;
        userPointRepository.findById(userId).ifPresent(p -> {
            p.setBalance(Math.max(0, p.getBalance() - 100));
            userPointRepository.save(p);
        });

        // 주문 상태 PAID 변경
        orderRepository.findByTxId(event.txId()).ifPresent(order -> {
            order.setStatus("PAID");
            orderRepository.save(order);
        });

        log.info("[PaymentWorker] 결제 완료 및 주문 PAID 확정: txId={}", event.txId());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recordCompensationOutboxEvent(Step4Dtos.SagaEventPayload event) {
        try {
            Step4Dtos.SagaEventPayload failPayload = new Step4Dtos.SagaEventPayload(
                    "PAYMENT_FAILED",
                    event.orderId(),
                    event.userId(),
                    event.productId(),
                    event.quantity(),
                    event.txId()
            );

            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .aggregateId(event.txId())
                    .payload(objectMapper.writeValueAsString(failPayload))
                    .status("INIT")
                    .createdAt(LocalDateTime.now())
                    .build();
            outboxRepository.save(outboxEvent);

        } catch (Exception e) {
            log.error("[PaymentWorker] 보상 Outbox 이벤트 저장 실패", e);
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
