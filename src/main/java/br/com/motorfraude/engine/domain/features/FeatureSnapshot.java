package br.com.motorfraude.engine.domain.features;

import br.com.motorfraude.contracts.EvaluateTransaction;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class FeatureSnapshot {

    private final Map<String, Object> values;

    private FeatureSnapshot(Map<String, Object> values) {
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
        return new FeatureSnapshot(v);
    }

    public Map<String, Object> values() {
        return values;
    }
}
