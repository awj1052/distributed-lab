package lab.step3_outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lab.domain.Orders;
import lab.domain.OutboxEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class Step3OrderService {

    private final Step3OrderRepository orderRepository;
    private final Step3OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * 주문 데이터만 DB에 독립적으로 커밋하는 내부 트랜잭션 메서드.
     */
    @Transactional
    public Orders saveOrderRecord(Step3Dtos.OrderRequest request, String defaultPrefix) {
        Long productId = request.productId() != null ? request.productId() : 1L;
        Long userId = request.userId() != null ? request.userId() : 1L;
        String txId = (request.txId() != null && !request.txId().isBlank())
                ? request.txId()
                : defaultPrefix + UUID.randomUUID();

        Orders order = Orders.builder()
                .userId(userId)
                .productId(productId)
                .status("PAID")
                .txId(txId)
                .build();

        return orderRepository.save(order);
    }

    /**
     * 엔드포인트 1: Dual-Write 장애 재현
     * DB 주문 저장은 이미 커밋된 후, 별도로 Kafka 발행을 시도하다 failRate(20%) 확률로 실패.
     * 결과: DB에는 주문이 저장되었으나 Kafka 메시지는 유실됨.
     */
    public Step3Dtos.OrderResponse createDualWriteOrder(Step3Dtos.OrderRequest request, double failRate) {
        // 1. DB에 주문 저장 (트랜잭션 커밋 완료!)
        Orders order = saveOrderRecord(request, "dw-");

        // 2. 외부 브로커(Kafka)로 이벤트 발행 시도 (20% 네트워크 장애 모의)
        if (Math.random() < failRate) {
            log.warn("[Dual-Write Failure] Kafka 브로커 장애 발생! DB에는 커밋되었으나 이벤트 영구 유실: txId={}", order.getTxId());
            throw new RuntimeException("Kafka Broker Network Failure (Dual-Write Dual-State Inconsistency, txId=" + order.getTxId() + ")");
        }

        // 3. 브로커 전송 성공 시
        sendToKafka(order);
        return new Step3Dtos.OrderResponse("SUCCESS", order.getId(), order.getTxId(), "Dual-write 주문 완료");
    }

    /**
     * 엔드포인트 2: Transactional Outbox 패턴
     * 단일 로컬 DB 트랜잭션 안에서 orders 와 outbox_event(status=INIT)를 원자적으로 함께 INSERT.
     * Kafka 직접 통신을 하지 않으므로 네트워크 장애와 무관하게 100% 원자성 보장.
     */
    @Transactional
    public Step3Dtos.OrderResponse createOutboxOrder(Step3Dtos.OrderRequest request) {
        Long productId = request.productId() != null ? request.productId() : 1L;
        Long userId = request.userId() != null ? request.userId() : 1L;
        String txId = (request.txId() != null && !request.txId().isBlank())
                ? request.txId()
                : "ob-" + UUID.randomUUID();

        // 1. 주문 엔티티 저장
        Orders order = Orders.builder()
                .userId(userId)
                .productId(productId)
                .status("PAID")
                .txId(txId)
                .build();
        orderRepository.save(order);

        // 2. 동일한 트랜잭션 내에서 Outbox 이벤트 INSERT (status=INIT)
        String payload = serialize(order);
        OutboxEvent outboxEvent = OutboxEvent.builder()
                .aggregateId(txId)
                .payload(payload)
                .status("INIT")
                .createdAt(LocalDateTime.now())
                .build();
        outboxRepository.save(outboxEvent);

        return new Step3Dtos.OrderResponse("ACCEPTED", order.getId(), txId, "Outbox 트랜잭션 원자 저장 완료");
    }

    private void sendToKafka(Orders order) {
        String payload = serialize(order);
        kafkaTemplate.send(Step3KafkaConfig.TOPIC_ORDER_CREATED_V3, order.getTxId(), payload);
    }

    private String serialize(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("직렬화 실패", e);
        }
    }
}
