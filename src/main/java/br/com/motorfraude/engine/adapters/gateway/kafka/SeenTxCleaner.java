package br.com.motorfraude.engine.adapters.gateway.kafka;

import java.time.Duration;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;

public final class SeenTxCleaner {

    public static final Duration RETENTION = Duration.ofHours(24);
    public static final int BATCH = 50_000;

    private final KeyValueStore<String, Long> store;
    private final Duration retention;
    private final int batch;
    private String cursor;

    public SeenTxCleaner(KeyValueStore<String, Long> store) {
        this(store, RETENTION, BATCH);
    }

    public SeenTxCleaner(KeyValueStore<String, Long> store, Duration retention, int batch) {
        this.store = store;
        this.retention = retention;
        this.batch = batch;
    }

    public int clean(long nowMillis) {
        long limit = nowMillis - retention.toMillis();
        int scanned = 0;
        int removed = 0;
        String last = null;
        boolean exhausted = true;
        try (var range = cursor == null ? store.all() : store.range(cursor, null)) {
            while (range.hasNext()) {
                if (scanned == batch) {
                    exhausted = false;
                    break;
                }
                KeyValue<String, Long> entry = range.next();
                scanned++;
                last = entry.key;
                if (entry.value != null && entry.value < limit) {
                    store.delete(entry.key);
                    removed++;
                }
            }
        }
        cursor = exhausted ? null : last;
        return removed;
    }
}
