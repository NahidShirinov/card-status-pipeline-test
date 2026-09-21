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
    private Long id;

    @Column(nullable = false)
    private String cardId;

    @Column(nullable = false)
    private String requestedStatus;

    @Column(nullable = false)
    private String result;

    @Column(nullable = false)
    private Instant processedAt;

    protected CardStatusRecord() {
    }

    public CardStatusRecord(String cardId, String requestedStatus, String result, Instant processedAt) {
        this.cardId = cardId;
        this.requestedStatus = requestedStatus;
        this.result = result;
        this.processedAt = processedAt;
    }

    public Long getId() {
        return id;
    }

    public String getCardId() {
        return cardId;
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
