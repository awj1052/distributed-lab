package lab;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class DistributedLabApplication {

    public static void main(String[] args) {
        SpringApplication.run(DistributedLabApplication.class, args);
    }
}
