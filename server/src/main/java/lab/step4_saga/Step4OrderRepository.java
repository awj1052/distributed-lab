package lab.step4_saga;

import lab.domain.Orders;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface Step4OrderRepository extends JpaRepository<Orders, Long> {
    Optional<Orders> findByTxId(String txId);
    long countByStatus(String status);
}
