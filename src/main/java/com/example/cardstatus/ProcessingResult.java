package com.example.cardstatus;

public record ProcessingResult(
        String id,
        String cardNumber,
        boolean success,
        long externalCallMs,
        long dbWriteMs,
        long kafkaPublishMs,
        long totalMs,
        String errorMessage
) {
}
