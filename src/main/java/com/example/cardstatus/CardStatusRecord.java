package com.example.cardstatus;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

import java.time.Instant;

@Entity
public class CardStatusRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long recordId;

    @Column(nullable = false)
    private String id;

    @Column(nullable = false)
    private String cardNumber;

    @Column(nullable = false)
    private String requestedStatus;

    @Column(nullable = false)
    private String result;

    @Column(nullable = false)
    private Instant processedAt;

    protected CardStatusRecord() {
    }

    public CardStatusRecord(String id, String cardNumber, String requestedStatus, String result, Instant processedAt) {
        this.id = id;
        this.cardNumber = cardNumber;
        this.requestedStatus = requestedStatus;
        this.result = result;
        this.processedAt = processedAt;
    }

    public Long getRecordId() {
        return recordId;
    }

    public String getId() {
        return id;
    }

    public String getCardNumber() {
        return cardNumber;
    }

    public String getRequestedStatus() {
        return requestedStatus;
    }

    public String getResult() {
        return result;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}
