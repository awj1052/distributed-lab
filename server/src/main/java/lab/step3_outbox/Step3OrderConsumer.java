package lab.step3_outbox;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class Step3OrderConsumer {

    @Getter
    private final AtomicLong consumedCount = new AtomicLong(0);

    @KafkaListener(topics = Step3KafkaConfig.TOPIC_ORDER_CREATED_V3, groupId = "lab-group-v3")
    public void consume(String payload) {
        long current = consumedCount.incrementAndGet();
        log.info("[Step3 Consumer] Kafka 이벤트 수신 완료 (누적: {}건)", current);
    }

    public void reset() {
        consumedCount.set(0);
    }
}
