package com.example.cardstatus;

public record StatsResponse(long outboxCount, long failureCount, long total, double successRatePercent) {
}
