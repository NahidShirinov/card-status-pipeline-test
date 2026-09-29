package com.example.cardstatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The other half of the outbox pattern: polls PENDING rows written by
 * CardStatusPersistenceService and actually sends them to Kafka, here -
 * decoupled from the HTTP request/response cycle - checking the send's
 * result instead of firing-and-forgetting it (KafkaTemplate.send()
 * returns a future; ignoring it means a broker-side failure disappears
 * silently, which was the second issue with the original design).
 *
 * A row stays PENDING (and gets retried on the next tick) until a send
 * actually succeeds, so a Kafka outage delays delivery instead of
 * losing events.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public OutboxPublisher(OutboxEventRepository repository,
                            KafkaTemplate<String, String> kafkaTemplate,
                            @Value("${app.kafka.topic}") String topic) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms:500}")
    public void publishPending() {
        List<OutboxEvent> pending = repository.findTop100ByStatusOrderByIdAsc(OutboxEvent.PENDING);
        for (OutboxEvent event : pending) {
            try {
                kafkaTemplate.send(topic, event.getCardId(), event.getPayload())
                        .get(5, TimeUnit.SECONDS);
                event.markSent();
                repository.save(event);
            } catch (Exception e) {
                event.recordFailedAttempt();
                repository.save(event);
                log.warn("Failed to publish outbox event {} (attempt {}): {}",
                        event.getEventId(), event.getAttempts(), e.getMessage());
            }
        }
    }
}
