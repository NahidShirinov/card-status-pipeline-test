package com.example.cardstatus;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

import java.time.Instant;

/**
 * Backs the optional Idempotency-Key request header on
 * POST /api/cards/status: a client that resends the same key gets back
 * the exact stored response instead of the pipeline reprocessing the
 * request.
 *
 * Deliberately keyed by a client-supplied key, not by the card's own
 * id - the same card can legitimately be processed again later (e.g.
 * blocked, then unblocked), which must not be treated as a duplicate.
 * Only a client that reuses the same idempotency key is asking for the
 * previous answer again.
 */
@Entity
public class IdempotencyRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String idempotencyKey;

    @Column(nullable = false, columnDefinition = "text")
    private String responseBody;

    @Column(nullable = false)
    private Instant createdAt;

    protected IdempotencyRecord() {
    }

    public IdempotencyRecord(String idempotencyKey, String responseBody) {
        this.idempotencyKey = idempotencyKey;
        this.responseBody = responseBody;
        this.createdAt = Instant.now();
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public String getResponseBody() {
        return responseBody;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
