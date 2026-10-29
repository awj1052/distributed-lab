package lab.step2_async;

import com.fasterxml.jackson.databind.ObjectMapper;
import lab.domain.Orders;
import lab.domain.ProcessedMessage;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@RequiredArgsConstructor
public class Step2OrderConsumer {

    private final ObjectMapper objectMapper;
    private final Step2ProcessedMessageRepository processedMessageRepository;
    private final Step2OrderRepository orderRepository;

    @Getter
    private final AtomicInteger processedCount = new AtomicInteger(0);
    @Getter
    private final AtomicInteger duplicateSkippedCount = new AtomicInteger(0);
    @Getter
    private final AtomicInteger duplicateExecutedCount = new AtomicInteger(0);

    /**
     * order-created-v2 토픽을 백그라운드에서 소비합니다.
     * 핵심: DB 트랜잭션 밖에서 외부 통신 지연(300ms)을 대기하므로 DB 커넥션을 전혀 점유하지 않습니다!
     */
    @KafkaListener(topics = Step2KafkaConfig.TOPIC_ORDER_CREATED, groupId = "lab-group-v2")
    public void consume(String messagePayload) {
        try {
            Step2Dtos.OrderEvent event = objectMapper.readValue(messagePayload, Step2Dtos.OrderEvent.class);

            // 1. 멱등성 검증 (idempotent=true 일 때)
            if (event.idempotent()) {
                boolean isNew = tryRecordProcessedMessage(event.txId());
                if (!isNew) {
                    log.warn("[Consumer-Idempotency] 멱등성 필터 작동: 이미 처리된 메시지이므로 SKIP (txId={})", event.txId());
                    duplicateSkippedCount.incrementAndGet();
                    return;
                }
            } else {
                duplicateExecutedCount.incrementAndGet();
            }

            // 2. 외부 API 지연 시뮬레이션 (300ms)
            // 중요: @Transactional 어노테이션이 없는 일반 메서드 구간이므로 HikariCP DB 커넥션을 전혀 물지 않음!
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // 3. 지연 완료 후 짧은 독립 트랜잭션으로 상태만 PAID 로 업데이트
            updateOrderStatusToPaid(event.orderId());
            processedCount.incrementAndGet();

        } catch (Exception e) {
            log.error("[Consumer Error] 메시지 처리 실패: {}", messagePayload, e);
        }
    }

    /**
     * 멱등성 테이블(processed_message)에 INSERT 시도.
     * 이미 동일한 messageId(txId)가 존재하면 PostgreSQL 23505 고유 키 위반으로 false 반환.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean tryRecordProcessedMessage(String txId) {
        try {
            ProcessedMessage record = ProcessedMessage.builder()
                    .messageId(txId)
                    .createdAt(LocalDateTime.now())
                    .build();
            processedMessageRepository.saveAndFlush(record);
            return true;
        } catch (DataIntegrityViolationException e) {
            return false;
        }
    }

    @Transactional
    public void updateOrderStatusToPaid(Long orderId) {
        if (orderId == null) return;
        orderRepository.findById(orderId).ifPresent(order -> {
            order.setStatus("PAID");
            orderRepository.save(order);
        });
    }
}
