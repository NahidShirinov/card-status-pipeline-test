package com.example.cardstatus;

import jakarta.validation.constraints.NotBlank;

public record CardStatusRequest(
        @NotBlank(message = "id must not be blank") String id,
        @NotBlank(message = "cardNumber must not be blank") String cardNumber,
        @NotBlank(message = "requestedStatus must not be blank") String requestedStatus) {
}
