package br.com.motorfraude.engine.adapters.gateway.kafka;

import br.com.motorfraude.contracts.BlocklistEntry;
import br.com.motorfraude.contracts.EntityType;
import br.com.motorfraude.contracts.EvaluateTransaction;
import br.com.motorfraude.engine.application.gateway.BlocklistReader;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public final class GlobalBlocklistReader implements BlocklistReader {

    private final ReadOnlyKeyValueStore<String, ValueAndTimestamp<BlocklistEntry>> store;

    public GlobalBlocklistReader(ReadOnlyKeyValueStore<String, ValueAndTimestamp<BlocklistEntry>> store) {
        this.store = store;
    }

    @Override
    public ListFlags flagsFor(EvaluateTransaction transaction) {
        boolean counterparty = listed(transaction.counterparty().accountToken(), EntityType.ACCOUNT);
        boolean device = transaction.deviceIdHash() != null && listed(transaction.deviceIdHash(), EntityType.DEVICE);
        return new ListFlags(counterparty, device);
    }

    private boolean listed(String token, EntityType type) {
        var stored = store.get(token);
        return stored != null && stored.value() != null && stored.value().entityType() == type;
    }
}
