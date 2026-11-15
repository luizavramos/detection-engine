package br.com.motorfraude.engine.fakes;

import br.com.motorfraude.contracts.CustomerProfile;
import br.com.motorfraude.engine.application.gateway.ProfileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class InMemoryProfileReader implements ProfileReader {

    private final Map<String, CustomerProfile> profiles = new HashMap<>();

    public void put(CustomerProfile profile) {
        profiles.put(profile.accountId(), profile);
    }

    @Override
    public Optional<CustomerProfile> find(String accountId) {
        return Optional.ofNullable(profiles.get(accountId));
    }
}
