package com.example.cardstatus;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Independent consumer (own consumer group) that only cares about
 * successful outcomes - the topic is the same one CardStatusEventProducer
 * publishes to, this just filters and persists its own view of it.
 */
@Component
public class OutboxConsumer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final OutboxRepository repository;

    public OutboxConsumer(OutboxRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "outbox-consumer")
    public void consume(String payload) throws Exception {
        CardStatusEvent event = MAPPER.readValue(payload, CardStatusEvent.class);
        if ("SUCCESS".equals(event.result())) {
            repository.save(new OutboxRecord(event.id(), event.cardNumber(), event.status(), event.result(), Instant.now()));
        }
    }
}
