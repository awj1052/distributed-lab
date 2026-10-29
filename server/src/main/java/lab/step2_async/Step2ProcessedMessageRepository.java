package lab.step2_async;

import lab.domain.ProcessedMessage;
import org.springframework.data.jpa.repository.JpaRepository;

public interface Step2ProcessedMessageRepository extends JpaRepository<ProcessedMessage, String> {
}
