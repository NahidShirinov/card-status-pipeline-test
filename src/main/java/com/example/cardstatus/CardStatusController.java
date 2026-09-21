package com.example.cardstatus;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/cards")
@Tag(name = "Card status", description = "Trigger and inspect card status processing")
public class CardStatusController {

    public record ChangeStatusRequest(String requestedStatus) {
    }

    private final CardStatusProcessingService processingService;
    private final CardStatusRepository repository;

    public CardStatusController(CardStatusProcessingService processingService,
                                 CardStatusRepository repository) {
        this.processingService = processingService;
        this.repository = repository;
    }

    @PostMapping("/{cardId}/status")
    @Operation(summary = "Process a single card status change through the full pipeline",
            description = "Calls the external status service, writes the result to Postgres, "
                    + "and publishes an event to Kafka - the same path a real uploaded-file row goes through.")
    public ProcessingResult changeStatus(@PathVariable String cardId,
                                          @RequestBody ChangeStatusRequest request) {
        return processingService.process(cardId, request.requestedStatus());
    }

    @GetMapping
    @Operation(summary = "List all processed card status records from the database")
    public List<CardStatusRecord> listProcessed() {
        return repository.findAll();
    }
}
