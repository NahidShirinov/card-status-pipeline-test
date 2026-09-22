package com.example.cardstatus;

import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * The pipeline under test: external status change call -> DB write -> Kafka publish.
 * Each stage is timed separately so a load test can find which stage is the bottleneck.
 */
@Service
public class CardStatusProcessingService {

    private static final int BATCH_CONCURRENCY = 20;

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

    public ProcessingResult process(String id, String cardNumber, String requestedStatus) {
        long start = System.nanoTime();
        long externalCallMs;
        String result;
        String errorMessage = null;

        long externalStart = System.nanoTime();
        try {
            Map<String, Object> response = externalClient.changeStatus(id, requestedStatus);
            result = response != null ? String.valueOf(response.get("result")) : "UNKNOWN";
        } catch (Exception e) {
            result = "FAILURE";
            errorMessage = e.getClass().getSimpleName() + ": " + e.getMessage();
        }
        externalCallMs = elapsedMs(externalStart);

        long dbStart = System.nanoTime();
        repository.save(new CardStatusRecord(id, cardNumber, requestedStatus, result, Instant.now()));
        long dbWriteMs = elapsedMs(dbStart);

        long kafkaStart = System.nanoTime();
        eventProducer.publish(id, cardNumber, requestedStatus, result);
        long kafkaPublishMs = elapsedMs(kafkaStart);

        long totalMs = elapsedMs(start);

        return new ProcessingResult(
                id,
                cardNumber,
                "SUCCESS".equals(result),
                externalCallMs,
                dbWriteMs,
                kafkaPublishMs,
                totalMs,
                errorMessage
        );
    }

    /**
     * Processes a batch (e.g. one uploaded file's worth of rows) concurrently,
     * bounded by BATCH_CONCURRENCY - mirrors how many parallel calls the real
     * external service would realistically tolerate.
     */
    public List<ProcessingResult> processBatch(List<CardStatusRequest> requests) {
        int poolSize = Math.min(BATCH_CONCURRENCY, Math.max(1, requests.size()));
        ExecutorService pool = Executors.newFixedThreadPool(poolSize);
        try {
            List<Future<ProcessingResult>> futures = requests.stream()
                    .map(r -> pool.submit(() -> process(r.id(), r.cardNumber(), r.requestedStatus())))
                    .toList();

            List<ProcessingResult> results = new ArrayList<>(futures.size());
            for (Future<ProcessingResult> future : futures) {
                results.add(future.get());
            }
            return results;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Batch processing was interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Batch processing failed", e.getCause());
        } finally {
            pool.shutdown();
        }
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }
}
