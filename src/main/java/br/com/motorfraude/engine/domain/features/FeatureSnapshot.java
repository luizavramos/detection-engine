package br.com.motorfraude.engine.domain.features;

import br.com.motorfraude.contracts.CustomerProfile;
import br.com.motorfraude.contracts.EvaluateTransaction;
import br.com.motorfraude.rules.kernel.evaluation.EvaluationContext;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public final class FeatureSnapshot {

    public static final String UNKNOWN_SEGMENT = "UNKNOWN";
    public static final long UNKNOWN = -1L;

    private final Instant occurredAt;
    private final boolean profileKnown;
    private final Map<String, Object> values;

    private FeatureSnapshot(Instant occurredAt, boolean profileKnown, Map<String, Object> values) {
        this.occurredAt = occurredAt;
        this.profileKnown = profileKnown;
        this.values = Collections.unmodifiableMap(values);
    }

    public static FeatureSnapshot of(EvaluateTransaction tx, AccountFeatures features, Baseline baseline, ZoneId zone) {
        var at = tx.occurredAt();
        var v = new LinkedHashMap<String, Object>();
        v.put("tx.amount", tx.amount().value());
        v.put("tx.type", tx.type().name());
        v.put("tx.channel", tx.channel().name());
        v.put("tx.hourOfDay", (long) at.atZone(zone).getHour());
        v.put("tx.counterpartyBank", tx.counterparty().bankIspb());

        v.put("features.count1m", features.count(at, Window.M1));
        v.put("features.count5m", features.count(at, Window.M5));
        v.put("features.count1h", features.count(at, Window.H1));
        v.put("features.count24h", features.count(at, Window.H24));
        v.put("features.count30d", features.count(at, Window.D30));
        v.put("features.sum1h", features.sum(at, Window.H1));
        v.put("features.sum24h", features.sum(at, Window.H24));
        v.put("features.avgAmount30d", baseline.avgAmount30d());
        v.put("features.maxAmount90d", baseline.maxAmount90d());
        v.put("features.distinctBeneficiaries24h", features.distinctBeneficiaries(at, Duration.ofHours(24)));
        v.put("features.isNewBeneficiary", baseline.newBeneficiary());
        v.put("features.isNewDevice", baseline.newDevice());
        return new FeatureSnapshot(at, false, v);
    }

    public FeatureSnapshot withProfile(Optional<CustomerProfile> profile) {
        var v = new LinkedHashMap<>(values);
        if (profile.isPresent()) {
            var p = profile.get();
            v.put("profile.segment", p.segment().name());
            v.put("profile.accountAgeDays", Math.max(0, Duration.between(p.accountOpenedAt(), occurredAt).toDays()));
            v.put("profile.hoursSinceCredentialChange", hoursSince(p.lastCredentialChangeAt()));
        } else {
            v.put("profile.segment", UNKNOWN_SEGMENT);
            v.put("profile.accountAgeDays", UNKNOWN);
            v.put("profile.hoursSinceCredentialChange", UNKNOWN);
        }
        return new FeatureSnapshot(occurredAt, profile.isPresent(), v);
    }

    public FeatureSnapshot withLists(boolean counterpartyFlagged, boolean deviceFlagged) {
        var v = new LinkedHashMap<>(values);
        v.put("lists.counterpartyFlagged", counterpartyFlagged);
        v.put("lists.deviceFlagged", deviceFlagged);
        return new FeatureSnapshot(occurredAt, profileKnown, v);
    }

    public boolean profileKnown() {
        return profileKnown;
    }

    public Map<String, Object> values() {
        return values;
    }

    public EvaluationContext toContext() {
        var builder = EvaluationContext.builder();
        values.forEach(builder::put);
        return builder.build();
    }

    private long hoursSince(Instant changedAt) {
        if (changedAt == null || changedAt.isAfter(occurredAt)) {
            return UNKNOWN;
        }
        return Duration.between(changedAt, occurredAt).toHours();
    }
}
