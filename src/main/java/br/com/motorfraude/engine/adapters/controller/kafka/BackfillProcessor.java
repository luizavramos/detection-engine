package br.com.motorfraude.engine.adapters.controller.kafka;

import br.com.motorfraude.contracts.TransactionHistoryRecord;
import br.com.motorfraude.engine.adapters.gateway.kafka.StateStoreDedupStore;
import br.com.motorfraude.engine.adapters.gateway.kafka.StateStoreFeatureStore;
import br.com.motorfraude.engine.application.gateway.DedupStore;
import br.com.motorfraude.engine.application.gateway.FeatureStore;
import br.com.motorfraude.engine.domain.features.AccountFeatures;
import br.com.motorfraude.engine.domain.features.Activity;
import java.time.Clock;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public final class BackfillProcessor implements Processor<String, TransactionHistoryRecord, Void, Void> {

    private final Clock clock;
    private DedupStore dedup;
    private FeatureStore features;

    public BackfillProcessor(Clock clock) {
        this.clock = clock;
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        KeyValueStore<String, Long> seen = context.getStateStore(EvaluateProcessor.SEEN_TX_STORE);
        KeyValueStore<String, AccountFeatures> store = context.getStateStore(EvaluateProcessor.FEATURES_STORE);
        this.dedup = new StateStoreDedupStore(seen);
        this.features = new StateStoreFeatureStore(store);
    }

    @Override
    public void process(Record<String, TransactionHistoryRecord> record) {
        var history = record.value();
        if (dedup.seen(history.transactionId())) {
            return;
        }
        dedup.mark(history.transactionId(), clock.instant());
        var accountFeatures = features.load(history.accountId());
        accountFeatures.record(new Activity(history.counterpartyToken(), history.deviceIdHash(), history.amount().value(), history.occurredAt()));
        features.save(history.accountId(), accountFeatures);
    }
}
