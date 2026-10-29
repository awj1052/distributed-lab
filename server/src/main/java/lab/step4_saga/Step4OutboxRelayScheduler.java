package lab.step4_saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import lab.domain.OutboxEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class Step4OutboxRelayScheduler {

    private final Step4OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * 0.5초마다 INIT 상태인 Outbox 이벤트를 읽어 Kafka의 알맞은 토픽으로 릴레이 발행합니다.
     */
    @Scheduled(fixedDelay = 500)
    @Transactional
    public void relaySagaEvents() {
        List<OutboxEvent> initEvents = outboxRepository.findTop100ByStatusOrderByIdAsc("INIT");
        if (initEvents.isEmpty()) {
            return;
        }

        for (OutboxEvent event : initEvents) {
            // Step 3 이벤트(ob-)는 건너뜀
            if (event.getAggregateId().startsWith("ob-")) {
                continue;
            }

            try {
                Step4Dtos.SagaEventPayload payload = objectMapper.readValue(event.getPayload(), Step4Dtos.SagaEventPayload.class);

                String targetTopic;
                if ("ORDER_CREATED".equalsIgnoreCase(payload.eventType())) {
                    targetTopic = Step4KafkaConfig.TOPIC_ORDER_CREATED;
                } else if ("PAYMENT_FAILED".equalsIgnoreCase(payload.eventType())) {
                    targetTopic = Step4KafkaConfig.TOPIC_PAYMENT_FAILED;
                } else {
                    targetTopic = "order-created-v3"; // 3단계 호환
                }

                kafkaTemplate.send(targetTopic, event.getAggregateId(), event.getPayload());
                event.setStatus("PUBLISHED");
                outboxRepository.save(event);

            } catch (Exception e) {
                log.error("[Saga Outbox Relay Error] 이벤트 릴레이 실패: eventId={}", event.getId(), e);
            }
        }
    }
}
