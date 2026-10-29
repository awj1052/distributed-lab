package lab.step4_saga;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class Step4KafkaConfig {

    public static final String TOPIC_FLASH_REQUESTS = "flash-order-requests";
    public static final String TOPIC_ORDER_CREATED = "order-created-v4";
    public static final String TOPIC_PAYMENT_FAILED = "payment-failed-v4";

    @Bean
    public NewTopic flashRequestsTopic() {
        return TopicBuilder.name(TOPIC_FLASH_REQUESTS).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic orderCreatedV4Topic() {
        return TopicBuilder.name(TOPIC_ORDER_CREATED).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic paymentFailedV4Topic() {
        return TopicBuilder.name(TOPIC_PAYMENT_FAILED).partitions(3).replicas(1).build();
    }
}
