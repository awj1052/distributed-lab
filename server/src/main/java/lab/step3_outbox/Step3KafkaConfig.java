package lab.step3_outbox;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class Step3KafkaConfig {

    public static final String TOPIC_ORDER_CREATED_V3 = "order-created-v3";

    @Bean
    public NewTopic orderCreatedV3Topic() {
        return TopicBuilder.name(TOPIC_ORDER_CREATED_V3)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
