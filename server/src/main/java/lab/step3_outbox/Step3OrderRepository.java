package lab.step3_outbox;

import lab.domain.Orders;
import org.springframework.data.jpa.repository.JpaRepository;

public interface Step3OrderRepository extends JpaRepository<Orders, Long> {
    long countByTxIdStartingWith(String prefix);
}
