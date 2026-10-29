package lab.step4_saga;

import lab.domain.UserPoint;
import org.springframework.data.jpa.repository.JpaRepository;

public interface Step4UserPointRepository extends JpaRepository<UserPoint, Long> {
}
