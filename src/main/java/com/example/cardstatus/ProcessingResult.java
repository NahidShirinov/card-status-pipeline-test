package com.example.cardstatus;

/**
 * dbWriteMs covers the whole transactional write - the card record and
 * its outbox event together (see CardStatusPersistenceService). There
 * is deliberately no "kafkaPublishMs": the actual Kafka send happens
 * later, off this request's thread (see OutboxPublisher), so nothing
 * about it can be timed synchronously here anymore.
 */
public record ProcessingResult(
        String id,
        String cardNumber,
        boolean success,
        long externalCallMs,
        long dbWriteMs,
        long totalMs,
        String errorMessage
) {
}
