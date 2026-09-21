package com.example.cardstatus;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class CardStatusEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public CardStatusEventProducer(KafkaTemplate<String, String> kafkaTemplate,
                                    @Value("${app.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void publish(String cardId, String status, String result) {
        String payload = """
                {"cardId":"%s","status":"%s","result":"%s"}""".formatted(cardId, status, result);
        kafkaTemplate.send(topic, cardId, payload);
    }
}
