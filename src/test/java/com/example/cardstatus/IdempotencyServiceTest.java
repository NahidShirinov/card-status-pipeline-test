package com.example.cardstatus;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IdempotencyServiceTest {

    private final IdempotencyRecordRepository repository = mock(IdempotencyRecordRepository.class);
    private final IdempotencyService service = new IdempotencyService(repository);

    @Test
    void firstCallInvokesActionAndStoresResult() {
        when(repository.findByIdempotencyKey("key-1")).thenReturn(Optional.empty());
        AtomicInteger calls = new AtomicInteger();

        ProcessingResult result = service.execute("key-1", ProcessingResult.class, () -> {
            calls.incrementAndGet();
            return sampleResult();
        });

        assertThat(calls.get()).isEqualTo(1);
        assertThat(result.id()).isEqualTo("card-1");
        verify(repository).save(any(IdempotencyRecord.class));
    }

    @Test
    void repeatedKeyReturnsStoredResultWithoutInvokingActionAgain() throws Exception {
        String storedJson = new ObjectMapper().writeValueAsString(sampleResult());
        when(repository.findByIdempotencyKey("key-2"))
                .thenReturn(Optional.of(new IdempotencyRecord("key-2", storedJson)));
        AtomicInteger calls = new AtomicInteger();

        ProcessingResult result = service.execute("key-2", ProcessingResult.class, () -> {
            calls.incrementAndGet();
            return sampleResult();
        });

        assertThat(calls.get()).isZero();
        assertThat(result.id()).isEqualTo("card-1");
        verify(repository, never()).save(any());
    }

    @Test
    void concurrentRaceOnANewKeyFallsBackToTheWinnersStoredResult() throws Exception {
        String storedJson = new ObjectMapper().writeValueAsString(sampleResult());
        when(repository.findByIdempotencyKey("key-3"))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(new IdempotencyRecord("key-3", storedJson)));
        doThrow(new DataIntegrityViolationException("duplicate key"))
                .when(repository).save(any(IdempotencyRecord.class));

        ProcessingResult result = service.execute("key-3", ProcessingResult.class, this::sampleResult);

        assertThat(result.id()).isEqualTo("card-1");
    }

    private ProcessingResult sampleResult() {
        return new ProcessingResult("card-1", "4111111111111111", true, 10, 5, 15, null);
    }
}
