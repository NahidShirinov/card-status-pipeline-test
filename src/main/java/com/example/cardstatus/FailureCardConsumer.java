package com.example.cardstatus;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Independent consumer (own consumer group) that only cares about
 * non-successful outcomes - lets failures be inspected/alerted on
 * without scanning the whole card_status_record table.
 */
@Component
public class FailureCardConsumer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final FailureCardRepository repository;

    public FailureCardConsumer(FailureCardRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "failurecards-consumer")
    public void consume(String payload) throws Exception {
        CardStatusEvent event = MAPPER.readValue(payload, CardStatusEvent.class);
        if (!"SUCCESS".equals(event.result())) {
            repository.save(new FailureCard(event.id(), event.cardNumber(), event.status(), event.result(), Instant.now()));
        }
    }
}
