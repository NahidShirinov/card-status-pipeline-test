package com.example.cardstatus;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

import java.time.Instant;

/**
 * A failed card-status event, as consumed from Kafka - lets you see
 * and later reprocess/alert on failures without scanning the whole
 * card_status_record table for result != SUCCESS.
 */
@Entity
public class FailureCard {

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

    protected FailureCard() {
    }

    public FailureCard(String cardId, String cardNumber, String status, String result, Instant receivedAt) {
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
