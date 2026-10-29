package lab.step2_async;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/v2/orders")
@RequiredArgsConstructor
public class Step2OrderController {

    private final Step2OrderService orderService;

    /**
     * 엔드포인트 1: 비동기 주문 접수 (202 Accepted 즉시 반환)
     */
    @PostMapping("/async")
    public ResponseEntity<Step2Dtos.AsyncOrderResponse> createOrderAsync(
            @RequestBody(required = false) Step2Dtos.AsyncOrderRequest request) {
        if (request == null) {
            request = new Step2Dtos.AsyncOrderRequest(1L, 1L, 1, null);
        }
        Step2Dtos.AsyncOrderResponse response = orderService.createOrderAsync(request);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    /**
     * 엔드포인트 2: 중복 메시지 발행 시 멱등성 검증 엔드포인트
     */
    @PostMapping("/duplicate-test")
    public ResponseEntity<Step2Dtos.AsyncOrderResponse> createDuplicateOrder(
            @RequestParam(defaultValue = "true") boolean idempotent,
            @RequestBody(required = false) Step2Dtos.AsyncOrderRequest request) {
        if (request == null) {
            request = new Step2Dtos.AsyncOrderRequest(1L, 1L, 1, null);
        }
        Step2Dtos.AsyncOrderResponse response = orderService.createDuplicateOrder(request, idempotent);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }
}
