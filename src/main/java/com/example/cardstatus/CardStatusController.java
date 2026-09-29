package com.example.cardstatus;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/cards")
@Tag(name = "Card status", description = "Trigger and inspect card status processing")
public class CardStatusController {

    private final CardStatusProcessingService processingService;
    private final CardStatusRepository repository;
    private final IdempotencyService idempotencyService;

    public CardStatusController(CardStatusProcessingService processingService,
                                 CardStatusRepository repository,
                                 IdempotencyService idempotencyService) {
        this.processingService = processingService;
        this.repository = repository;
        this.idempotencyService = idempotencyService;
    }

    @PostMapping("/status")
    @Operation(summary = "Process a single card status change through the full pipeline",
            description = "Calls the external status service and writes the result to Postgres "
                    + "(plus an outbox event for Kafka - see OutboxPublisher). Pass an optional "
                    + "Idempotency-Key header to make a retried request return the original result "
                    + "instead of reprocessing.")
    public ProcessingResult changeStatus(@Valid @RequestBody CardStatusRequest request,
                                          @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return processingService.process(request.id(), request.cardNumber(), request.requestedStatus());
        }
        return idempotencyService.execute(idempotencyKey, ProcessingResult.class,
                () -> processingService.process(request.id(), request.cardNumber(), request.requestedStatus()));
    }

    @PostMapping("/status/batch")
    @Operation(summary = "Process up to a whole uploaded file's worth of rows in one request",
            description = "Runs every item through the full pipeline concurrently (bounded pool) "
                    + "and returns each one's result and per-stage timing, in the same order they "
                    + "were sent. Capped at " + CardStatusProcessingService.MAX_BATCH_SIZE + " items per request.")
    public List<ProcessingResult> changeStatusBatch(@Valid @RequestBody List<@Valid CardStatusRequest> requests) {
        return processingService.processBatch(requests);
    }

    @GetMapping
    @Operation(summary = "List processed card status records from the database, paginated")
    public Page<CardStatusRecord> listProcessed(Pageable pageable) {
        return repository.findAll(pageable);
    }
}
