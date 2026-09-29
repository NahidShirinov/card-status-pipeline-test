package com.example.cardstatus;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

import java.time.Instant;

/**
 * A successfully processed card-status event, as consumed from Kafka
 * (not written directly by the pipeline) - this table only reflects
 * what a downstream consumer actually saw on the topic.
 *
 * Named SuccessCard, not "outbox" - the outbox pattern (OutboxEvent)
 * is a different thing: the write-side guarantee that a DB change and
 * its Kafka event are never inconsistent. This table is a read-side
 * projection built by a consumer, unrelated to that guarantee.
 *
 * eventId is unique so a Kafka redelivery (e.g. after a rebalance,
 * before the previous poll's offsets were committed) is a no-op
 * instead of a duplicate row - Kafka only guarantees at-least-once
 * delivery, so the consumer has to be idempotent itself.
 */
@Entity
public class SuccessCard {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long recordId;

    @Column(nullable = false, unique = true)
    private String eventId;

    @Column(nullable = false)
    private String cardId;

    @Column(nullable = false)
    private String cardNumber;

    @Column(nullable = false)
    private String status;

    @Column(nullable = false)
    private String result;

    @Column(nullable = false)
    private Instant receivedAt;

    protected SuccessCard() {
    }

    public SuccessCard(String eventId, String cardId, String cardNumber, String status, String result, Instant receivedAt) {
        this.eventId = eventId;
        this.cardId = cardId;
        this.cardNumber = cardNumber;
        this.status = status;
        this.result = result;
        this.receivedAt = receivedAt;
    }

    public Long getRecordId() {
        return recordId;
    }

    public String getEventId() {
        return eventId;
    }

    public String getCardId() {
        return cardId;
    }

    public String getCardNumber() {
        return cardNumber;
    }

    public String getStatus() {
        return status;
    }

    public String getResult() {
        return result;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }
}
