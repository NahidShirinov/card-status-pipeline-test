package com.example.cardstatus;

/**
 * Shape of the JSON payload published by CardStatusEventProducer to
 * the card-status-events topic - shared by every consumer of that topic.
 */
public record CardStatusEvent(String id, String cardNumber, String status, String result) {
}
