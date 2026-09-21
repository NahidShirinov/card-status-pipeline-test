package com.example.cardstatus;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

/**
 * Calls the same contract implemented by the WireMock mock in mock-service/:
 * POST /api/cards/status {cardId, requestedStatus} -> {cardId, status, result, processedAt}
 */
@Component
public class ExternalCardStatusClient {

    private final RestTemplate restTemplate;
    private final String baseUrl;

    public ExternalCardStatusClient(RestTemplate restTemplate,
                                     @Value("${external.card-status.base-url}") String baseUrl) {
        this.restTemplate = restTemplate;
        this.baseUrl = baseUrl;
    }

    public Map<String, Object> changeStatus(String cardId, String requestedStatus) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> body = Map.of(
                "cardId", cardId,
                "requestedStatus", requestedStatus
        );

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(body, headers);

        @SuppressWarnings("unchecked")
        Map<String, Object> response = restTemplate.exchange(
                baseUrl + "/api/cards/status",
                HttpMethod.POST,
                request,
                Map.class
        ).getBody();

        return response;
    }
}
