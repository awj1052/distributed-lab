package lab.step3_outbox;

public class Step3Dtos {

    public record OrderRequest(
            Long productId,
            Long userId,
            Integer quantity,
            String txId
    ) {}

    public record OrderResponse(
            String status,
            Long orderId,
            String txId,
            String message
    ) {}

    public record Step3VerifyResponse(
            long dbOrderCount,
            long outboxInitCount,
            long outboxPublishedCount,
            long consumerReceivedCount,
            boolean isConsistent
    ) {}
}
