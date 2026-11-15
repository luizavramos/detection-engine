package br.com.motorfraude.engine.fakes;

import br.com.motorfraude.engine.adapters.gateway.kafka.AccountFeaturesSerde;
import br.com.motorfraude.engine.application.gateway.FeatureStore;
import br.com.motorfraude.engine.domain.features.AccountFeatures;
import java.util.HashMap;
import java.util.Map;

public final class InMemoryFeatureStore implements FeatureStore {

    private final Map<String, byte[]> stored = new HashMap<>();

    @Override
    public AccountFeatures load(String accountId) {
        var bytes = stored.get(accountId);
        return bytes == null ? new AccountFeatures() : AccountFeaturesSerde.deserialize(bytes);
    }

    @Override
    public void save(String accountId, AccountFeatures features) {
        stored.put(accountId, AccountFeaturesSerde.serialize(features));
    }
}
