package br.com.motorfraude.engine;

import br.com.motorfraude.contracts.Channel;
import br.com.motorfraude.contracts.Counterparty;
import br.com.motorfraude.contracts.CustomerProfile;
import br.com.motorfraude.contracts.EvaluateTransaction;
import br.com.motorfraude.contracts.Money;
import br.com.motorfraude.contracts.Segment;
import br.com.motorfraude.contracts.TransactionType;
import br.com.motorfraude.engine.domain.features.AccountFeatures;
import br.com.motorfraude.engine.domain.features.Activity;
import br.com.motorfraude.engine.domain.features.FeatureSnapshot;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

public final class TestData {

    public static final ZoneId SAO_PAULO = ZoneId.of("America/Sao_Paulo");
    public static final Instant NOW = Instant.parse("2026-11-10T15:00:30Z");
    public static final String ACCOUNT = "acc_tok_00000001";

    private TestData() {
    }

    public static EvaluateTransaction tx(String transactionId, Instant when, long amount, String counterparty, String device) {
        return new EvaluateTransaction(1, transactionId, ACCOUNT, "cus_tok_00000001", TransactionType.PIX, new Money(amount, "BRL"),
                new Counterparty(counterparty, "60746948"), Channel.MOBILE, device, when);
    }

    public static EvaluateTransaction tx(String transactionId, long amount) {
        return tx(transactionId, NOW, amount, "acc_tok_dest0001", "dev_aaaaaaaa");
    }

    public static Activity activity(EvaluateTransaction tx) {
        return new Activity(tx.counterparty().accountToken(), tx.deviceIdHash(), tx.amount().value(), tx.occurredAt());
    }

    public static FeatureSnapshot snapshot(EvaluateTransaction tx) {
        var features = new AccountFeatures();
        var baseline = features.baselineFor(activity(tx));
        features.record(activity(tx));
        return FeatureSnapshot.of(tx, features, baseline, SAO_PAULO);
    }

    public static CustomerProfile profile(Segment segment, Instant opened, Instant credentialChange) {
        return new CustomerProfile(ACCOUNT, segment, opened, credentialChange, List.of("dev_aaaaaaaa"), NOW);
    }
}
