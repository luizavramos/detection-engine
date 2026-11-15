package br.com.motorfraude.engine.application;

import br.com.motorfraude.contracts.ContextCompleteness;
import br.com.motorfraude.contracts.EvaluateTransaction;
import br.com.motorfraude.engine.application.gateway.BlocklistReader;
import br.com.motorfraude.engine.application.gateway.DedupStore;
import br.com.motorfraude.engine.application.gateway.FeatureStore;
import br.com.motorfraude.engine.application.gateway.ProfileReader;
import br.com.motorfraude.engine.application.gateway.RuleSetProvider;
import br.com.motorfraude.engine.domain.Decision;
import br.com.motorfraude.engine.domain.features.Activity;
import br.com.motorfraude.engine.domain.features.FeatureSnapshot;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

public final class EvaluateTransactionUseCase {

    public static final ZoneId BUSINESS_ZONE = ZoneId.of("America/Sao_Paulo");

    private final DedupStore dedup;
    private final FeatureStore featureStore;
    private final ProfileReader profiles;
    private final BlocklistReader blocklist;
    private final RuleSetProvider ruleSets;
    private final Clock clock;

    public EvaluateTransactionUseCase(DedupStore dedup, FeatureStore featureStore, ProfileReader profiles, BlocklistReader blocklist,
            RuleSetProvider ruleSets, Clock clock) {
        this.dedup = dedup;
        this.featureStore = featureStore;
        this.profiles = profiles;
        this.blocklist = blocklist;
        this.ruleSets = ruleSets;
        this.clock = clock;
    }

    public Outcome execute(EvaluateTransaction tx, Instant receivedAt, String traceId) {
        var ruleSet = ruleSets.current().orElseThrow(() -> new IllegalStateException("nenhum conjunto de regras carregado"));

        if (dedup.seen(tx.transactionId())) {
            return Outcome.duplicateOf();
        }
        dedup.mark(tx.transactionId(), clock.instant());

        var activity = new Activity(tx.counterparty().accountToken(), tx.deviceIdHash(), tx.amount().value(), tx.occurredAt());
        var features = featureStore.load(tx.accountId());
        var baseline = features.baselineFor(activity);
        features.record(activity);
        featureStore.save(tx.accountId(), features);

        var profile = profiles.find(tx.accountId());
        var flags = blocklist.flagsFor(tx);
        var snapshot = FeatureSnapshot.of(tx, features, baseline, BUSINESS_ZONE)
                .withProfile(profile)
                .withLists(flags.counterpartyFlagged(), flags.deviceFlagged());

        var evaluation = ruleSet.evaluate(snapshot.toContext());
        var completeness = profile.isPresent() ? ContextCompleteness.FULL : ContextCompleteness.PARTIAL;
        var decision = Decision.from(tx, evaluation, snapshot, completeness, receivedAt, clock.instant(), traceId);
        return Outcome.decided(decision);
    }
}
