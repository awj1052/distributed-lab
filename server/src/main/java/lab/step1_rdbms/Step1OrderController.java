package lab.step1_rdbms;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class Step1OrderController {

    private final Step1OrderService orderService;

    /**
     * 엔드포인트 1: Hot Row 비관적 락 경합
     */
    @PostMapping("/hot-lock")
    public ResponseEntity<Step1Dtos.OrderResponse> orderWithHotLock(@RequestBody(required = false) Step1Dtos.OrderRequest request) {
        if (request == null) {
            request = new Step1Dtos.OrderRequest(1L, 1L, 1, 0);
        }
        Step1Dtos.OrderResponse response = orderService.orderWithHotLock(request);
        return ResponseEntity.ok(response);
    }

    /**
     * 엔드포인트 2: 트랜잭션 내 외부 API 지연으로 인한 커넥션 풀 고갈 유발
     */
    @PostMapping("/external-delay")
    public ResponseEntity<Step1Dtos.OrderResponse> orderWithExternalDelay(@RequestBody(required = false) Step1Dtos.OrderRequest request) {
        if (request == null) {
            request = new Step1Dtos.OrderRequest(1L, 1L, 1, 0);
        }
        Step1Dtos.OrderResponse response = orderService.orderWithExternalDelay(request);
        return ResponseEntity.ok(response);
    }

    /**
     * 엔드포인트 3: 다중 리소스 잠금 순서 역전으로 인한 데드락 유발
     */
    @PostMapping("/deadlock")
    public ResponseEntity<Step1Dtos.OrderResponse> orderWithDeadlock(@RequestBody(required = false) Step1Dtos.DeadlockOrderRequest request) {
        if (request == null) {
            request = new Step1Dtos.DeadlockOrderRequest(1L, 2L, 1L);
        }
        Step1Dtos.OrderResponse response = orderService.orderWithDeadlock(request);
        return ResponseEntity.ok(response);
    }

    /**
     * 엔드포인트 4: 락 순서 오름차순 정렬을 통한 데드락 해결
     */
    @PostMapping("/deadlock-fixed")
    public ResponseEntity<Step1Dtos.OrderResponse> orderWithDeadlockFixed(@RequestBody(required = false) Step1Dtos.DeadlockOrderRequest request) {
        if (request == null) {
            request = new Step1Dtos.DeadlockOrderRequest(1L, 2L, 1L);
        }
        Step1Dtos.OrderResponse response = orderService.orderWithDeadlockFixed(request);
        return ResponseEntity.ok(response);
    }
}
