package lab.step2_async;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class Step2KafkaConfig {

    public static final String TOPIC_ORDER_CREATED = "order-created-v2";

    @Bean
    public NewTopic orderCreatedV2Topic() {
        return TopicBuilder.name(TOPIC_ORDER_CREATED)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
