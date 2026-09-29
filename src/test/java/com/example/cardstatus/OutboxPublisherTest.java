package com.example.cardstatus;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxPublisherTest {

    private final OutboxEventRepository repository = mock(OutboxEventRepository.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    private final OutboxPublisher publisher = new OutboxPublisher(repository, kafkaTemplate, "card-status-events");

    @Test
    @SuppressWarnings("unchecked")
    void successfulSendMarksEventSent() {
        OutboxEvent event = new OutboxEvent("event-1", "card-1", "{}");
        when(repository.findTop100ByStatusOrderByIdAsc(OutboxEvent.PENDING)).thenReturn(List.of(event));
        when(kafkaTemplate.send(eq("card-status-events"), eq("card-1"), eq("{}")))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        publisher.publishPending();

        assertThat(event.getStatus()).isEqualTo(OutboxEvent.SENT);
        assertThat(event.getAttempts()).isZero();
        verify(repository).save(event);
    }

    @Test
    void failedSendLeavesEventPendingAndRecordsAnAttempt() {
        OutboxEvent event = new OutboxEvent("event-2", "card-2", "{}");
        when(repository.findTop100ByStatusOrderByIdAsc(OutboxEvent.PENDING)).thenReturn(List.of(event));
        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker down"));
        when(kafkaTemplate.send(eq("card-status-events"), eq("card-2"), eq("{}"))).thenReturn(failed);

        publisher.publishPending();

        assertThat(event.getStatus()).isEqualTo(OutboxEvent.PENDING);
        assertThat(event.getAttempts()).isEqualTo(1);
        verify(repository).save(event);
    }
}
