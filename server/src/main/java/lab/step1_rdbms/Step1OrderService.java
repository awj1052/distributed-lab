package lab.step1_rdbms;

import lab.domain.Orders;
import lab.domain.Product;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class Step1OrderService {

    private final ProductRepository productRepository;
    private final OrdersRepository ordersRepository;

    /**
     * 엔드포인트 1: Hot Row 비관적 락 경합
     * 트랜잭션 내 20ms 지연으로 락 유지 시간을 증가시켜 대기열을 발생시킴.
     */
    @Transactional
    public Step1Dtos.OrderResponse orderWithHotLock(Step1Dtos.OrderRequest request) {
        Long productId = request.productId() != null ? request.productId() : 1L;
        Long userId = request.userId() != null ? request.userId() : 1L;
        int qty = request.quantity() != null ? request.quantity() : 1;

        Product product = productRepository.findByIdWithPessimisticLock(productId)
                .orElseThrow(() -> new IllegalArgumentException("상품이 존재하지 않습니다: " + productId));

        if (product.getStock() < qty) {
            throw new IllegalStateException("재고가 부족합니다. 현재 재고: " + product.getStock());
        }

        product.setStock(product.getStock() - qty);

        Orders order = Orders.builder()
                .userId(userId)
                .productId(productId)
                .status("PAID")
                .txId("v1-hot-" + UUID.randomUUID())
                .build();
        ordersRepository.save(order);

        // 락 경합 체감을 위한 의도적 지연 (20ms)
        try {
            Thread.sleep(20);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return new Step1Dtos.OrderResponse("SUCCESS", order.getId(), productId, product.getStock(), "주문 완료");
    }

    /**
     * 엔드포인트 2: 외부 API 지연으로 인한 커넥션 풀 고갈
     * 트랜잭션 내부에서 가짜 외부 결제/알림 API 호출(300ms 지연)을 수행하여 DB 커넥션 반환을 지연시킴.
     */
    @Transactional
    public Step1Dtos.OrderResponse orderWithExternalDelay(Step1Dtos.OrderRequest request) {
        Long productId = request.productId() != null ? request.productId() : 1L;
        Long userId = request.userId() != null ? request.userId() : 1L;
        int qty = request.quantity() != null ? request.quantity() : 1;

        Product product = productRepository.findByIdWithPessimisticLock(productId)
                .orElseThrow(() -> new IllegalArgumentException("상품이 존재하지 않습니다: " + productId));

        if (product.getStock() < qty) {
            throw new IllegalStateException("재고가 부족합니다. 현재 재고: " + product.getStock());
        }

        product.setStock(product.getStock() - qty);

        Orders order = Orders.builder()
                .userId(userId)
                .productId(productId)
                .status("PAID")
                .txId("v1-delay-" + UUID.randomUUID())
                .build();
        ordersRepository.save(order);

        // 트랜잭션 내부에서 외부 API 호출을 가정한 지연 (300ms) -> HikariCP Pool 고갈 유발!
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return new Step1Dtos.OrderResponse("SUCCESS", order.getId(), productId, product.getStock(), "외부 API 연동 주문 완료");
    }

    /**
     * 엔드포인트 3: 다중 리소스 잠금 순서 역전으로 인한 데드락 유발
     * 50% 확률로 [A -> B], 50% 확률로 [B -> A] 순서로 락을 획득하여 PostgreSQL 40P01 데드락 발생.
     */
    @Transactional
    public Step1Dtos.OrderResponse orderWithDeadlock(Step1Dtos.DeadlockOrderRequest request) {
        Long idA = request.firstProductId() != null ? request.firstProductId() : 1L;
        Long idB = request.secondProductId() != null ? request.secondProductId() : 2L;
        Long userId = request.userId() != null ? request.userId() : 1L;

        // 50% 확률로 교차 잠금 시도
        boolean swap = Math.random() < 0.5;
        Long firstId = swap ? idB : idA;
        Long secondId = swap ? idA : idB;

        // 1번째 상품 락 획득
        Product p1 = productRepository.findByIdWithPessimisticLock(firstId)
                .orElseThrow(() -> new IllegalArgumentException("상품 없음: " + firstId));
        p1.setStock(Math.max(0, p1.getStock() - 1));

        // 두 스레드가 서로 1번째 락을 쥐고 대기하도록 유도
        try {
            Thread.sleep(15);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 2번째 상품 락 획득 시도 -> 교차 대기로 인한 데드락(40P01) 발생
        Product p2 = productRepository.findByIdWithPessimisticLock(secondId)
                .orElseThrow(() -> new IllegalArgumentException("상품 없음: " + secondId));
        p2.setStock(Math.max(0, p2.getStock() - 1));

        Orders order = Orders.builder()
                .userId(userId)
                .productId(firstId)
                .status("PAID")
                .txId("v1-deadlock-" + UUID.randomUUID())
                .build();
        ordersRepository.save(order);

        return new Step1Dtos.OrderResponse("SUCCESS", order.getId(), firstId, p1.getStock(), "묶음 상품 주문 완료");
    }

    /**
     * 엔드포인트 4: 락 순서 오름차순 정렬을 통한 데드락 해결
     * 상품 ID를 항상 작은 것부터 정렬하여 락을 획득함으로써 순환 대기를 원천 차단.
     */
    @Transactional
    public Step1Dtos.OrderResponse orderWithDeadlockFixed(Step1Dtos.DeadlockOrderRequest request) {
        Long idA = request.firstProductId() != null ? request.firstProductId() : 1L;
        Long idB = request.secondProductId() != null ? request.secondProductId() : 2L;
        Long userId = request.userId() != null ? request.userId() : 1L;

        // 락 획득 순서를 항상 ID 오름차순으로 고정!
        Long firstId = Math.min(idA, idB);
        Long secondId = Math.max(idA, idB);

        Product p1 = productRepository.findByIdWithPessimisticLock(firstId)
                .orElseThrow(() -> new IllegalArgumentException("상품 없음: " + firstId));
        p1.setStock(Math.max(0, p1.getStock() - 1));

        try {
            Thread.sleep(15);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        Product p2 = productRepository.findByIdWithPessimisticLock(secondId)
                .orElseThrow(() -> new IllegalArgumentException("상품 없음: " + secondId));
        p2.setStock(Math.max(0, p2.getStock() - 1));

        Orders order = Orders.builder()
                .userId(userId)
                .productId(firstId)
                .status("PAID")
                .txId("v1-fixed-" + UUID.randomUUID())
                .build();
        ordersRepository.save(order);

        return new Step1Dtos.OrderResponse("SUCCESS", order.getId(), firstId, p1.getStock(), "정렬 잠금 주문 완료");
    }
}
