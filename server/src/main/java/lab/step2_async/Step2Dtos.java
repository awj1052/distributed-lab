package lab.step2_async;

public class Step2Dtos {

    public record AsyncOrderRequest(
            Long productId,
            Long userId,
            Integer quantity,
            String txId
    ) {}

    public record AsyncOrderResponse(
            String status,
            Long orderId,
            String txId,
            String message
    ) {}

    public record OrderEvent(
            Long orderId,
            Long userId,
            Long productId,
            Integer quantity,
            String txId,
            boolean idempotent
    ) {}
}
