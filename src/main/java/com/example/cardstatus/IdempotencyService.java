package com.example.cardstatus;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import java.util.function.Supplier;

/**
 * Generic "run this once per idempotency key" wrapper: replays the
 * stored response on a repeat key instead of invoking the action again.
 * A unique constraint on the key resolves the race where two requests
 * with the same brand-new key arrive concurrently - whichever save()
 * loses just falls back to reading what the winner stored.
 */
@Service
public class IdempotencyService {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final IdempotencyRecordRepository repository;

    public IdempotencyService(IdempotencyRecordRepository repository) {
        this.repository = repository;
    }

    public <T> T execute(String idempotencyKey, Class<T> responseType, Supplier<T> action) {
        var existing = repository.findByIdempotencyKey(idempotencyKey);
        if (existing.isPresent()) {
            return readValue(existing.get().getResponseBody(), responseType);
        }

        T result = action.get();
        try {
            repository.save(new IdempotencyRecord(idempotencyKey, writeValue(result)));
        } catch (DataIntegrityViolationException e) {
            // Lost a race with a concurrent request using the same key -
            // the action already ran twice, but at least surface the
            // winner's stored response for consistency going forward.
            return repository.findByIdempotencyKey(idempotencyKey)
                    .map(r -> readValue(r.getResponseBody(), responseType))
                    .orElse(result);
        }
        return result;
    }

    private static <T> String writeValue(T value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize idempotent response", e);
        }
    }

    private static <T> T readValue(String json, Class<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize stored idempotent response", e);
        }
    }
}
