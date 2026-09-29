package com.example.cardstatus;

public record StatsResponse(long successCount, long failureCount, long total, double successRatePercent) {
}
