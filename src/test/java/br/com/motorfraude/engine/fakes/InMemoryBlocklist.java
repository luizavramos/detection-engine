package br.com.motorfraude.engine.fakes;

import br.com.motorfraude.contracts.EvaluateTransaction;
import br.com.motorfraude.engine.application.gateway.BlocklistReader;
import java.util.HashSet;
import java.util.Set;

public final class InMemoryBlocklist implements BlocklistReader {

    private final Set<String> accounts = new HashSet<>();
    private final Set<String> devices = new HashSet<>();

    public void flagAccount(String token) {
        accounts.add(token);
    }

    public void flagDevice(String hash) {
        devices.add(hash);
    }

    @Override
    public ListFlags flagsFor(EvaluateTransaction transaction) {
        return new ListFlags(accounts.contains(transaction.counterparty().accountToken()),
                transaction.deviceIdHash() != null && devices.contains(transaction.deviceIdHash()));
    }
}
