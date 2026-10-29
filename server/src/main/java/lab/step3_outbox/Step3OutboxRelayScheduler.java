package lab.step3_outbox;

import lab.domain.OutboxEvent;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class Step3OutboxRelayScheduler {

    private final Step3OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Getter
    @Setter
    private double relayFailRate = 0.2; // 20% 브로커 일시 장애 시뮬레이션

    /**
     * 0.5초(500ms)마다 INIT 상태의 Outbox 이벤트를 폴링하여 Kafka로 발행.
     * 20% 전송 실패가 발생해도 다음 주기에 다시 조회되어 재시도되므로 최종 100% 발행 보장!
     */
    @Scheduled(fixedDelay = 500)
    @Transactional
    public void relayOutboxEvents() {
        List<OutboxEvent> initEvents = outboxRepository.findTop100ByStatusOrderByIdAsc("INIT");
        if (initEvents.isEmpty()) {
            return;
        }

        int successCount = 0;
        int failedCount = 0;

        for (OutboxEvent event : initEvents) {
            // Step 4 이벤트는 Step 4 릴레이 스케줄러가 처리하도록 분리
            if (!event.getAggregateId().startsWith("ob-")) {
                continue;
            }

            // 20% 확률로 브로커 일시 장애 모의
            if (Math.random() < relayFailRate) {
                log.warn("[Outbox Relay] Kafka 브로커 일시 장애로 발행 실패 -> 다음 주기에 재시도 대기: eventId={}", event.getId());
                failedCount++;
                continue; // 상태를 INIT으로 유지하여 다음 500ms 주기에 재시도!
            }

            // Kafka 발행 성공 시 상태를 PUBLISHED로 전환
            kafkaTemplate.send(Step3KafkaConfig.TOPIC_ORDER_CREATED_V3, event.getAggregateId(), event.getPayload());
            event.setStatus("PUBLISHED");
            outboxRepository.save(event);
            successCount++;
        }

        log.info("[Outbox Relay] 폴링 배치 완료: 성공={}건, 일시실패(재시도대기)={}건, 남은INIT={}",
                successCount, failedCount, outboxRepository.countByStatus("INIT"));
    }
}
