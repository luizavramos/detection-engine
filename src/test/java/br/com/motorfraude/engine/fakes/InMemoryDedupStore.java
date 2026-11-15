package br.com.motorfraude.engine.fakes;

import br.com.motorfraude.engine.application.gateway.DedupStore;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public final class InMemoryDedupStore implements DedupStore {

    private final Map<String, Instant> seen = new HashMap<>();

    @Override
    public boolean seen(String transactionId) {
        return seen.containsKey(transactionId);
    }

    @Override
    public void mark(String transactionId, Instant at) {
        seen.put(transactionId, at);
    }

    public Instant markedAt(String transactionId) {
        return seen.get(transactionId);
    }
}
