package br.com.motorfraude.engine.application.gateway;

import br.com.motorfraude.engine.domain.features.AccountFeatures;

public interface FeatureStore {

    AccountFeatures load(String accountId);

    void save(String accountId, AccountFeatures features);
}
