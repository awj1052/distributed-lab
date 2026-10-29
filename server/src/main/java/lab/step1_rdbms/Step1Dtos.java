package lab.step1_rdbms;

public class Step1Dtos {

    public record OrderRequest(
            Long productId,
            Long userId,
            Integer quantity,
            Integer reqId
    ) {}

    public record DeadlockOrderRequest(
            Long firstProductId,
            Long secondProductId,
            Long userId
    ) {}

    public record OrderResponse(
            String status,
            Long orderId,
            Long productId,
            int remainingStock,
            String message
    ) {}
}
