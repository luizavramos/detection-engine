package br.com.motorfraude.engine.adapters.controller.kafka;

import br.com.motorfraude.commons.tracing.StandardHeaders;
import br.com.motorfraude.commons.tracing.TraceParent;
import br.com.motorfraude.contracts.BlocklistEntry;
import br.com.motorfraude.contracts.CustomerProfile;
import br.com.motorfraude.contracts.EvaluateTransaction;
import br.com.motorfraude.engine.adapters.gateway.kafka.GlobalBlocklistReader;
import br.com.motorfraude.engine.adapters.gateway.kafka.KTableProfileReader;
import br.com.motorfraude.engine.adapters.gateway.kafka.SeenTxCleaner;
import br.com.motorfraude.engine.adapters.gateway.kafka.StateStoreDedupStore;
import br.com.motorfraude.engine.adapters.gateway.kafka.StateStoreFeatureStore;
import br.com.motorfraude.engine.application.EvaluateTransactionUseCase;
import br.com.motorfraude.engine.application.Outcome;
import br.com.motorfraude.engine.application.gateway.RuleSetProvider;
import br.com.motorfraude.engine.domain.features.AccountFeatures;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public final class EvaluateProcessor implements Processor<String, EvaluateTransaction, String, Outcome> {

    public static final String SEEN_TX_STORE = "seen-tx";
    public static final String FEATURES_STORE = "account-features";
    public static final String PROFILE_STORE = "customer-profile-store";
    public static final String BLOCKLIST_STORE = "blocklist-store";
    public static final Duration CLEANUP_INTERVAL = Duration.ofMinutes(5);

    private final RuleSetProvider ruleSets;
    private final Clock clock;
    private ProcessorContext<String, Outcome> context;
    private EvaluateTransactionUseCase useCase;

    public EvaluateProcessor(RuleSetProvider ruleSets, Clock clock) {
        this.ruleSets = ruleSets;
        this.clock = clock;
    }

    @Override
    public void init(ProcessorContext<String, Outcome> context) {
        this.context = context;
        KeyValueStore<String, Long> seen = context.getStateStore(SEEN_TX_STORE);
        KeyValueStore<String, AccountFeatures> features = context.getStateStore(FEATURES_STORE);
        KeyValueStore<String, ValueAndTimestamp<CustomerProfile>> profiles = context.getStateStore(PROFILE_STORE);
        KeyValueStore<String, ValueAndTimestamp<BlocklistEntry>> blocklist = context.getStateStore(BLOCKLIST_STORE);
        this.useCase = new EvaluateTransactionUseCase(new StateStoreDedupStore(seen), new StateStoreFeatureStore(features),
                new KTableProfileReader(profiles), new GlobalBlocklistReader(blocklist), ruleSets, clock);
        var cleaner = new SeenTxCleaner(seen);
        context.schedule(CLEANUP_INTERVAL, PunctuationType.WALL_CLOCK_TIME, cleaner::clean);
    }

    @Override
    public void process(Record<String, EvaluateTransaction> record) {
        var receivedAt = receivedAt(record.headers(), record.timestamp());
        var outcome = useCase.execute(record.value(), receivedAt, traceId(record.headers()));
        if (!outcome.duplicate()) {
            context.forward(record.withValue(outcome));
        }
    }

    private static Instant receivedAt(Headers headers, long recordTimestamp) {
        var header = headers.lastHeader(StandardHeaders.RECEIVED_AT);
        if (header != null) {
            try {
                return Instant.parse(new String(header.value(), StandardCharsets.UTF_8));
            } catch (DateTimeParseException ignored) {
                return Instant.ofEpochMilli(recordTimestamp);
            }
        }
        return Instant.ofEpochMilli(recordTimestamp);
    }

    private static String traceId(Headers headers) {
        var header = headers.lastHeader(StandardHeaders.TRACEPARENT);
        if (header == null) {
            return null;
        }
        return TraceParent.parse(new String(header.value(), StandardCharsets.UTF_8)).map(TraceParent::traceId).orElse(null);
    }
}
