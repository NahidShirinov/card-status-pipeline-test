package com.example.cardstatus;

public record ProcessingResult(
        String cardId,
        boolean success,
        long externalCallMs,
        long dbWriteMs,
        long kafkaPublishMs,
        long totalMs,
        String errorMessage
) {
}
