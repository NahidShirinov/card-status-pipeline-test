package com.example.cardstatus;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class CardStatusProcessingServiceTest {

    @Test
    void batchLargerThanMaxSizeIsRejected() {
        CardStatusProcessingService service = new CardStatusProcessingService(
                mock(ExternalCardStatusClient.class),
                mock(CardStatusPersistenceService.class),
                Executors.newSingleThreadExecutor());

        List<CardStatusRequest> tooMany = Collections.nCopies(
                CardStatusProcessingService.MAX_BATCH_SIZE + 1,
                new CardStatusRequest("id", "4111111111111111", "BLOCKED"));

        assertThatThrownBy(() -> service.processBatch(tooMany))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds the maximum");
    }
}
