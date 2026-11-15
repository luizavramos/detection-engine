package br.com.motorfraude.engine.adapters.gateway.kafka;

import br.com.motorfraude.contracts.CustomerProfile;
import br.com.motorfraude.engine.application.gateway.ProfileReader;
import java.util.Optional;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public final class KTableProfileReader implements ProfileReader {

    private final ReadOnlyKeyValueStore<String, ValueAndTimestamp<CustomerProfile>> store;

    public KTableProfileReader(ReadOnlyKeyValueStore<String, ValueAndTimestamp<CustomerProfile>> store) {
        this.store = store;
    }

    @Override
    public Optional<CustomerProfile> find(String accountId) {
        return Optional.ofNullable(store.get(accountId)).map(ValueAndTimestamp::value);
    }
}
