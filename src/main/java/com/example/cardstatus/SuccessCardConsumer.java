package com.example.cardstatus;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Independent consumer (own consumer group) that only cares about
 * successful outcomes - the topic is the same one the outbox publisher
 * sends to, this just filters and persists its own view of it.
 *
 * Idempotent by eventId, since Kafka only guarantees at-least-once
 * delivery (e.g. a redelivery after a rebalance before the previous
 * poll's offset was committed).
 */
@Component
public class SuccessCardConsumer {

    private static final Logger log = LoggerFactory.getLogger(SuccessCardConsumer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final SuccessCardRepository repository;

    public SuccessCardConsumer(SuccessCardRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "success-card-consumer")
    public void consume(String payload) throws Exception {
        CardStatusEvent event = MAPPER.readValue(payload, CardStatusEvent.class);
        if (!"SUCCESS".equals(event.result())) {
            return;
        }
        try {
            repository.save(new SuccessCard(
                    event.eventId(), event.id(), event.cardNumber(), event.status(), event.result(), Instant.now()));
        } catch (DataIntegrityViolationException e) {
            log.info("Duplicate delivery of event {} ignored", event.eventId());
        }
    }
}
