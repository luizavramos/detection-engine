package br.com.motorfraude.engine.application.gateway;

import br.com.motorfraude.contracts.CustomerProfile;
import java.util.Optional;

public interface ProfileReader {

    Optional<CustomerProfile> find(String accountId);
}
