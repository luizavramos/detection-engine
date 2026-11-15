package br.com.motorfraude.engine.domain.features;

import java.time.Instant;
import java.util.Objects;

public record Activity(String counterpartyToken, String deviceHash, long amountCents, Instant occurredAt) {

    public Activity {
        Objects.requireNonNull(counterpartyToken, "counterpartyToken");
        Objects.requireNonNull(occurredAt, "occurredAt");
    }

    boolean hasDevice() {
        return deviceHash != null && !deviceHash.isBlank();
    }
}
