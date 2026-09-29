package com.example.cardstatus;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * The pipeline under test: external status change call -> transactional
 * DB write (record + outbox event, see CardStatusPersistenceService).
 * Each stage is timed separately so a load test can find which stage
 * is the bottleneck. The actual Kafka send happens later, out of this
 * request path entirely - see OutboxPublisher.
 */
@Service
public class CardStatusProcessingService {

    public static final int MAX_BATCH_SIZE = 1000;

    private final ExternalCardStatusClient externalClient;
    private final CardStatusPersistenceService persistenceService;
    private final ExecutorService batchExecutor;

    public CardStatusProcessingService(ExternalCardStatusClient externalClient,
                                        CardStatusPersistenceService persistenceService,
                                        ExecutorService batchExecutor) {
        this.externalClient = externalClient;
        this.persistenceService = persistenceService;
        this.batchExecutor = batchExecutor;
    }

    public ProcessingResult process(String id, String cardNumber, String requestedStatus) {
        long start = System.nanoTime();
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
        long externalCallMs = elapsedMs(externalStart);

        long dbStart = System.nanoTime();
        persistenceService.saveWithOutboxEvent(id, cardNumber, requestedStatus, result);
        long dbWriteMs = elapsedMs(dbStart);

        long totalMs = elapsedMs(start);

        return new ProcessingResult(
                id,
                cardNumber,
                "SUCCESS".equals(result),
                externalCallMs,
                dbWriteMs,
                totalMs,
                errorMessage
        );
    }

    /**
     * Processes a batch (e.g. one uploaded file's worth of rows) on the
     * application's single shared, bounded executor - not a new thread
     * pool per call, which is what let N concurrent batch requests spawn
     * N unrelated pools and, between them, far more threads than the
     * process was ever sized for.
     */
    public List<ProcessingResult> processBatch(List<CardStatusRequest> requests) {
        if (requests.size() > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    "Batch size %d exceeds the maximum of %d".formatted(requests.size(), MAX_BATCH_SIZE));
        }
        try {
            List<Future<ProcessingResult>> futures = requests.stream()
                    .map(r -> batchExecutor.submit(() -> process(r.id(), r.cardNumber(), r.requestedStatus())))
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
        }
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }
}
