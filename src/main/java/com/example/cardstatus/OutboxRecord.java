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
 */
@Entity
public class OutboxRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long recordId;

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

    protected OutboxRecord() {
    }

    public OutboxRecord(String cardId, String cardNumber, String status, String result, Instant receivedAt) {
        this.cardId = cardId;
        this.cardNumber = cardNumber;
        this.status = status;
        this.result = result;
        this.receivedAt = receivedAt;
    }

    public Long getRecordId() {
        return recordId;
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
