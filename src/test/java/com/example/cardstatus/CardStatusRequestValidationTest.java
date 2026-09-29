package com.example.cardstatus;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class CardStatusRequestValidationTest {

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    void blankFieldsAreRejected() {
        CardStatusRequest request = new CardStatusRequest("", "", "");
        Set<ConstraintViolation<CardStatusRequest>> violations = validator.validate(request);
        assertThat(violations).hasSize(3);
    }

    @Test
    void nullFieldsAreRejected() {
        CardStatusRequest request = new CardStatusRequest(null, null, null);
        assertThat(validator.validate(request)).hasSize(3);
    }

    @Test
    void validRequestHasNoViolations() {
        CardStatusRequest request = new CardStatusRequest("id-1", "4111111111111111", "BLOCKED");
        assertThat(validator.validate(request)).isEmpty();
    }
}
