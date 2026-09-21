package com.example.cardstatus;

import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;

/**
 * The pipeline under test: external status change call -> DB write -> Kafka publish.
 * Each stage is timed separately so a load test can find which stage is the bottleneck.
 */
@Service
public class CardStatusProcessingService {

    private final ExternalCardStatusClient externalClient;
    private final CardStatusRepository repository;
    private final CardStatusEventProducer eventProducer;

    public CardStatusProcessingService(ExternalCardStatusClient externalClient,
                                        CardStatusRepository repository,
                                        CardStatusEventProducer eventProducer) {
        this.externalClient = externalClient;
        this.repository = repository;
        this.eventProducer = eventProducer;
    }

    public ProcessingResult process(String cardId, String requestedStatus) {
        long start = System.nanoTime();
        long externalCallMs;
        String result;
        String errorMessage = null;

        long externalStart = System.nanoTime();
        try {
            Map<String, Object> response = externalClient.changeStatus(cardId, requestedStatus);
            result = response != null ? String.valueOf(response.get("result")) : "UNKNOWN";
        } catch (Exception e) {
            result = "FAILURE";
            errorMessage = e.getClass().getSimpleName() + ": " + e.getMessage();
        }
        externalCallMs = elapsedMs(externalStart);

        long dbStart = System.nanoTime();
        repository.save(new CardStatusRecord(cardId, requestedStatus, result, Instant.now()));
        long dbWriteMs = elapsedMs(dbStart);

        long kafkaStart = System.nanoTime();
        eventProducer.publish(cardId, requestedStatus, result);
        long kafkaPublishMs = elapsedMs(kafkaStart);

        long totalMs = elapsedMs(start);

        return new ProcessingResult(
                cardId,
                "SUCCESS".equals(result),
                externalCallMs,
                dbWriteMs,
                kafkaPublishMs,
                totalMs,
                errorMessage
        );
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }
}
