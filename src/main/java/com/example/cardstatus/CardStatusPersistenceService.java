package com.example.cardstatus;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Owns the one database transaction that must never be split in two:
 * the processed-card row and the outbox event describing it are
 * written together, or neither is. This is what makes "the pipeline
 * says a card was processed" and "an event for it will eventually
 * reach Kafka" a single fact instead of two facts that a crash
 * between them could make disagree (the "dual write" problem you get
 * from writing to the DB and then separately calling
 * KafkaTemplate.send() from the request path).
 *
 * A separate bean (not just a private method) so @Transactional goes
 * through Spring's proxy - calling it via "this." from within the same
 * class would silently skip the proxy and the transaction.
 */
@Service
public class CardStatusPersistenceService {

    private final CardStatusRepository cardStatusRepository;
    private final OutboxEventRepository outboxEventRepository;

    public CardStatusPersistenceService(CardStatusRepository cardStatusRepository,
                                         OutboxEventRepository outboxEventRepository) {
        this.cardStatusRepository = cardStatusRepository;
        this.outboxEventRepository = outboxEventRepository;
    }

    @Transactional
    public void saveWithOutboxEvent(String id, String cardNumber, String requestedStatus, String result) {
        cardStatusRepository.save(new CardStatusRecord(id, cardNumber, requestedStatus, result, Instant.now()));

        String eventId = UUID.randomUUID().toString();
        String payload = """
                {"eventId":"%s","id":"%s","cardNumber":"%s","status":"%s","result":"%s"}"""
                .formatted(eventId, id, cardNumber, requestedStatus, result);
        outboxEventRepository.save(new OutboxEvent(eventId, id, payload));
    }
}
