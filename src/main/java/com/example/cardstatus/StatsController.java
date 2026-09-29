package com.example.cardstatus;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/stats")
@Tag(name = "Stats", description = "Monitoring counters built from the outbox/failure Kafka consumers")
public class StatsController {

    private final SuccessCardRepository successCardRepository;
    private final FailureCardRepository failureCardRepository;

    public StatsController(SuccessCardRepository successCardRepository, FailureCardRepository failureCardRepository) {
        this.successCardRepository = successCardRepository;
        this.failureCardRepository = failureCardRepository;
    }

    @GetMapping
    @Operation(summary = "Success/failure counts as seen by the Kafka consumers",
            description = "Reflects what actually landed in the success_card and failure_card tables via "
                    + "the two independent consumers, not the card_status_record table written directly by the pipeline.")
    public StatsResponse stats() {
        long successCount = successCardRepository.count();
        long failureCount = failureCardRepository.count();
        long total = successCount + failureCount;
        double successRate = total == 0 ? 0.0 : (successCount * 100.0) / total;
        return new StatsResponse(successCount, failureCount, total, successRate);
    }
}
