package lab.step2_async;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lab.domain.Orders;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class Step2OrderService {

    private final Step2OrderRepository orderRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * 엔드포인트 1: 비동기 주문 접수
     * DB 트랜잭션은 PENDING 상태 INSERT (수 ms)만 처리하고 즉시 커밋 및 커넥션 반납.
     * 이후 Kafka 로 메시지를 던지고 202 응답.
     */
    @Transactional
    public Step2Dtos.AsyncOrderResponse createOrderAsync(Step2Dtos.AsyncOrderRequest request) {
        Long productId = request.productId() != null ? request.productId() : 1L;
        Long userId = request.userId() != null ? request.userId() : 1L;
        int qty = request.quantity() != null ? request.quantity() : 1;
        String txId = (request.txId() != null && !request.txId().isBlank())
                ? request.txId()
                : "tx-v2-" + UUID.randomUUID();

        // 1. 최소한의 로컬 DB 작업 (PENDING 저장)
        Orders order = Orders.builder()
                .userId(userId)
                .productId(productId)
                .status("PENDING")
                .txId(txId)
                .build();
        orderRepository.save(order);

        // 2. 비동기 처리를 위한 Kafka 메시지 발행 (후속 처리는 백그라운드 컨슈머에게 위임)
        Step2Dtos.OrderEvent event = new Step2Dtos.OrderEvent(
                order.getId(),
                userId,
                productId,
                qty,
                txId,
                true // 기본 비동기 주문은 멱등성 보장
        );

        publishToKafka(event);

        return new Step2Dtos.AsyncOrderResponse("ACCEPTED", order.getId(), txId, "주문 접수 완료 (비동기 처리 중)");
    }

    /**
     * 엔드포인트 2: 멱등성 검증용 중복 발행 시뮬레이션
     * 의도적으로 동일한 txId 메시지를 Kafka 에 2회 연속 발행하여 중복 수신 환경 조성.
     */
    @Transactional
    public Step2Dtos.AsyncOrderResponse createDuplicateOrder(Step2Dtos.AsyncOrderRequest request, boolean idempotent) {
        Long productId = request.productId() != null ? request.productId() : 1L;
        Long userId = request.userId() != null ? request.userId() : 1L;
        int qty = request.quantity() != null ? request.quantity() : 1;
        String txId = (request.txId() != null && !request.txId().isBlank())
                ? request.txId()
                : "dup-" + UUID.randomUUID();

        Orders order = Orders.builder()
                .userId(userId)
                .productId(productId)
                .status("PENDING")
                .txId(txId)
                .build();
        orderRepository.save(order);

        Step2Dtos.OrderEvent event = new Step2Dtos.OrderEvent(
                order.getId(),
                userId,
                productId,
                qty,
                txId,
                idempotent
        );

        // 동일한 메시지를 2번 연속 발행 (네트워크 재시도/중복 전달 시뮬레이션)
        publishToKafka(event);
        publishToKafka(event);

        return new Step2Dtos.AsyncOrderResponse("ACCEPTED", order.getId(), txId, "중복 메시지 2회 발행 완료 (idempotent=" + idempotent + ")");
    }

    private void publishToKafka(Step2Dtos.OrderEvent event) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(Step2KafkaConfig.TOPIC_ORDER_CREATED, event.txId(), payload);
        } catch (JsonProcessingException e) {
            log.error("[Kafka Publish Error] 이벤트 직렬화 실패: {}", event, e);
            throw new RuntimeException("Kafka 메시지 직렬화 실패", e);
        }
    }
}
