package br.com.motorfraude.engine.infrastructure;

import br.com.motorfraude.contracts.BlocklistEntry;
import br.com.motorfraude.contracts.CustomerProfile;
import br.com.motorfraude.engine.adapters.controller.kafka.BackfillProcessor;
import br.com.motorfraude.engine.adapters.controller.kafka.EvaluateProcessor;
import br.com.motorfraude.engine.adapters.gateway.kafka.AccountFeaturesSerde;
import br.com.motorfraude.engine.adapters.gateway.kafka.EngineSerdes;
import br.com.motorfraude.engine.application.gateway.EngineMetrics;
import br.com.motorfraude.engine.application.gateway.RuleSetProvider;
import java.time.Clock;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

public final class TopologyFactory {

    public static final String COMMANDS_TOPIC = "transaction-commands.v1";
    public static final String BACKFILL_TOPIC = "transaction-history-backfill.v1";
    public static final String PROFILES_TOPIC = "customer-profile.v1";
    public static final String BLOCKLIST_TOPIC = "blocklist.v1";
    public static final String DECISIONS_TOPIC = "fraud-decisions.v1";
    public static final String ALERTS_TOPIC = "fraud-alerts.v1";
    public static final String DLQ_TOPIC = "transaction-commands.dlq";

    private TopologyFactory() {
    }

    public static Topology create(RuleSetProvider ruleSets, Clock clock, EngineMetrics metrics) {
        return create(ruleSets, clock, metrics, false);
    }

    static Topology create(RuleSetProvider ruleSets, Clock clock, EngineMetrics metrics, boolean inMemory) {
        var builder = new StreamsBuilder();
        var earliest = org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

        builder.addStateStore(Stores.keyValueStoreBuilder(store(EvaluateProcessor.SEEN_TX_STORE, inMemory), Serdes.String(), Serdes.Long()));
        builder.addStateStore(Stores.keyValueStoreBuilder(store(EvaluateProcessor.FEATURES_STORE, inMemory), Serdes.String(),
                AccountFeaturesSerde.kafkaSerde()));

        builder.table(PROFILES_TOPIC, Consumed.with(Serdes.String(), EngineSerdes.profile()).withOffsetResetPolicy(earliest),
                Materialized.<String, CustomerProfile>as(store(EvaluateProcessor.PROFILE_STORE, inMemory))
                        .withKeySerde(Serdes.String()).withValueSerde(EngineSerdes.profile()));
        builder.globalTable(BLOCKLIST_TOPIC, Consumed.with(Serdes.String(), EngineSerdes.blocklist()).withOffsetResetPolicy(earliest),
                Materialized.<String, BlocklistEntry>as(store(EvaluateProcessor.BLOCKLIST_STORE, inMemory))
                        .withKeySerde(Serdes.String()).withValueSerde(EngineSerdes.blocklist()));

        var outcomes = builder
                .stream(COMMANDS_TOPIC, Consumed.with(Serdes.String(), EngineSerdes.command()).withOffsetResetPolicy(earliest))
                .process(() -> new EvaluateProcessor(ruleSets, clock, metrics), EvaluateProcessor.SEEN_TX_STORE, EvaluateProcessor.FEATURES_STORE,
                        EvaluateProcessor.PROFILE_STORE);

        outcomes.mapValues(outcome -> outcome.decision().decisionMessage())
                .to(DECISIONS_TOPIC, Produced.with(Serdes.String(), EngineSerdes.decision()));
        outcomes.filter((accountId, outcome) -> outcome.decision().isAlert())
                .mapValues(outcome -> outcome.decision().alertMessage().orElseThrow())
                .to(ALERTS_TOPIC, Produced.with(Serdes.String(), EngineSerdes.alert()));

        builder.stream(BACKFILL_TOPIC, Consumed.with(Serdes.String(), EngineSerdes.history()).withOffsetResetPolicy(earliest))
                .process(() -> new BackfillProcessor(clock), EvaluateProcessor.SEEN_TX_STORE, EvaluateProcessor.FEATURES_STORE);

        return builder.build();
    }

    private static KeyValueBytesStoreSupplier store(String name, boolean inMemory) {
        return inMemory ? Stores.inMemoryKeyValueStore(name) : Stores.persistentKeyValueStore(name);
    }
}
