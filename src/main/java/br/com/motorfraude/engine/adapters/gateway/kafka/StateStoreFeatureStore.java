package br.com.motorfraude.engine.adapters.gateway.kafka;

import br.com.motorfraude.engine.application.gateway.FeatureStore;
import br.com.motorfraude.engine.domain.features.AccountFeatures;
import org.apache.kafka.streams.state.KeyValueStore;

public final class StateStoreFeatureStore implements FeatureStore {

    private final KeyValueStore<String, AccountFeatures> store;

    public StateStoreFeatureStore(KeyValueStore<String, AccountFeatures> store) {
        this.store = store;
    }

    @Override
    public AccountFeatures load(String accountId) {
        var stored = store.get(accountId);
        return stored == null ? new AccountFeatures() : stored;
    }

    @Override
    public void save(String accountId, AccountFeatures features) {
        store.put(accountId, features);
    }
}
