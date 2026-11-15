package br.com.motorfraude.engine.adapters.gateway.kafka;

import br.com.motorfraude.engine.application.gateway.DedupStore;
import java.time.Instant;
import org.apache.kafka.streams.state.KeyValueStore;

public final class StateStoreDedupStore implements DedupStore {

    private final KeyValueStore<String, Long> store;

    public StateStoreDedupStore(KeyValueStore<String, Long> store) {
        this.store = store;
    }

    @Override
    public boolean seen(String transactionId) {
        return store.get(transactionId) != null;
    }

    @Override
    public void mark(String transactionId, Instant at) {
        store.put(transactionId, at.toEpochMilli());
    }
}
