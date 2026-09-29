package com.example.cardstatus;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

import java.time.Instant;

/**
 * Transactional outbox row: written in the SAME database transaction
 * as the CardStatusRecord it describes (see CardStatusPersistenceService),
 * so "the card was processed" and "an event for it exists to be published"
 * can never disagree - unlike calling KafkaTemplate.send() directly from
 * the request path, where a crash between the DB write and the Kafka
 * send silently drops the event.
 *
 * OutboxPublisher polls PENDING rows and does the actual Kafka send,
 * decoupled from the request/response cycle.
 */
@Entity
public class OutboxEvent {

    public static final String PENDING = "PENDING";
    public static final String SENT = "SENT";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String eventId;

    @Column(nullable = false)
    private String cardId;

    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Column(nullable = false)
    private String status;

    @Column(nullable = false)
    private Instant createdAt;

    private Instant sentAt;

    @Column(nullable = false)
    private int attempts;

    protected OutboxEvent() {
    }

    public OutboxEvent(String eventId, String cardId, String payload) {
        this.eventId = eventId;
        this.cardId = cardId;
        this.payload = payload;
        this.status = PENDING;
        this.createdAt = Instant.now();
        this.attempts = 0;
    }

    public void markSent() {
        this.status = SENT;
        this.sentAt = Instant.now();
    }

    public void recordFailedAttempt() {
        this.attempts++;
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public String getCardId() {
        return cardId;
    }

    public String getPayload() {
        return payload;
    }

    public String getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getSentAt() {
        return sentAt;
    }

    public int getAttempts() {
        return attempts;
    }
}
