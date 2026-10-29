package lab.step2_async;

import lab.domain.Orders;
import org.springframework.data.jpa.repository.JpaRepository;

public interface Step2OrderRepository extends JpaRepository<Orders, Long> {
}
