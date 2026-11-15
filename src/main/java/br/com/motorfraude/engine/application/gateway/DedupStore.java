package br.com.motorfraude.engine.application.gateway;

import java.time.Instant;

public interface DedupStore {

    boolean seen(String transactionId);

    void mark(String transactionId, Instant at);
}
