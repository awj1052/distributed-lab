package lab.step4_saga;

public class Step4Dtos {

    public record FlashOrderRequest(
            Long productId,
            Long userId,
            Integer quantity,
            String txId
    ) {}

    public record FlashOrderResponse(
            String status,
            String txId,
            String message
    ) {}

    public record SagaEventPayload(
            String eventType, // ORDER_CREATED or PAYMENT_FAILED
            Long orderId,
            Long userId,
            Long productId,
            Integer quantity,
            String txId
    ) {}

    public record SagaVerifyResponse(
            int initialStock,
            int currentStock,
            int deductedStock,
            long paidOrdersCount,
            long cancelledOrdersCount,
            long pendingOrdersCount,
            long outboxInitCount,
            boolean isStockAndPaidConsistent,
            boolean isOutboxDrained,
            boolean isConsistent
    ) {}
}
