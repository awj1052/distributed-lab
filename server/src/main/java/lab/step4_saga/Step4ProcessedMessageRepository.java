package lab.step4_saga;

import lab.domain.ProcessedMessage;
import org.springframework.data.jpa.repository.JpaRepository;

public interface Step4ProcessedMessageRepository extends JpaRepository<ProcessedMessage, String> {
}
