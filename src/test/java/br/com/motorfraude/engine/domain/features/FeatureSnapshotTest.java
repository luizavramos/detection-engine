package br.com.motorfraude.engine.domain.features;

import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.contracts.Channel;
import br.com.motorfraude.contracts.Counterparty;
import br.com.motorfraude.contracts.EvaluateTransaction;
import br.com.motorfraude.contracts.Money;
import br.com.motorfraude.contracts.TransactionType;
import br.com.motorfraude.rules.kernel.catalog.FeatureCatalog;
import br.com.motorfraude.rules.kernel.catalog.FeatureType;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FeatureSnapshotTest {

    private static final ZoneId SAO_PAULO = ZoneId.of("America/Sao_Paulo");
    private static final Instant T = Instant.parse("2026-11-11T02:30:00Z");

    private static EvaluateTransaction tx(Instant when, long amount, String counterparty, String device) {
        return new EvaluateTransaction(1, "tx-" + when.toEpochMilli(), "acc_tok_00000001", "cus_tok_00000001", TransactionType.PIX,
                new Money(amount, "BRL"), new Counterparty(counterparty, "60746948"), Channel.MOBILE, device, when);
    }

    private static Activity activity(EvaluateTransaction tx) {
        return new Activity(tx.counterparty().accountToken(), tx.deviceIdHash(), tx.amount().value(), tx.occurredAt());
    }

    private static FeatureSnapshot evaluate(AccountFeatures features, EvaluateTransaction tx) {
        var baseline = features.baselineFor(activity(tx));
        features.record(activity(tx));
        return FeatureSnapshot.of(tx, features, baseline, SAO_PAULO);
    }

    @Test
    @DisplayName("os campos da transação: valor em centavos, tipo, canal, banco da contraparte e hora em São Paulo")
    void camposDaTransacao() {
        var snapshot = evaluate(new AccountFeatures(), tx(T, 150_000, "acc_tok_dest0001", "dev_aaaaaaaa"));

        assertThat(snapshot.values()).containsEntry("tx.amount", 150_000L).containsEntry("tx.type", "PIX")
                .containsEntry("tx.channel", "MOBILE").containsEntry("tx.counterpartyBank", "60746948");
        assertThat(snapshot.values().get("tx.hourOfDay")).isEqualTo(23L);
    }

    @Test
    @DisplayName("a hora do dia segue America/Sao_Paulo, não UTC")
    void horaEmSaoPaulo() {
        var utcMidday = Instant.parse("2026-11-11T15:10:00Z");

        assertThat(evaluate(new AccountFeatures(), tx(utcMidday, 100, "acc_tok_dest0001", "dev_aaaaaaaa")).values().get("tx.hourOfDay"))
                .isEqualTo(12L);
    }

    @Test
    @DisplayName("as 12 features do catálogo saem com os valores certos")
    void featuresDoCatalogo() {
        var features = new AccountFeatures();
        for (int day = 1; day <= 30; day++) {
            features.record(activity(tx(T.minus(Duration.ofDays(day)), 20_000, "acc_tok_dest0001", "dev_aaaaaaaa")));
        }
        features.record(activity(tx(T.minus(Duration.ofDays(60)), 90_000, "acc_tok_dest0001", "dev_aaaaaaaa")));
        features.record(activity(tx(T.minus(Duration.ofMinutes(3)), 1_000, "acc_tok_dest0001", "dev_aaaaaaaa")));

        var snapshot = evaluate(features, tx(T, 200_000, "acc_tok_dest0009", "dev_zzzzzzzz"));
        var v = snapshot.values();

        assertThat(v).containsEntry("features.count1m", 1L).containsEntry("features.count5m", 2L).containsEntry("features.count1h", 2L)
                .containsEntry("features.count24h", 2L).containsEntry("features.count30d", 31L)
                .containsEntry("features.sum1h", 201_000L).containsEntry("features.sum24h", 201_000L)
                .containsEntry("features.maxAmount90d", 90_000L).containsEntry("features.distinctBeneficiaries24h", 2L)
                .containsEntry("features.isNewBeneficiary", true).containsEntry("features.isNewDevice", true);
        assertThat((Long) v.get("features.avgAmount30d")).isEqualTo((29 * 20_000L + 1_000L) / 30);
    }

    @Test
    @DisplayName("dispositivo e beneficiário já conhecidos não são novos")
    void conhecidos() {
        var features = new AccountFeatures();
        evaluate(features, tx(T.minusSeconds(60), 100, "acc_tok_dest0001", "dev_aaaaaaaa"));

        var snapshot = evaluate(features, tx(T, 100, "acc_tok_dest0001", "dev_aaaaaaaa"));

        assertThat(snapshot.values()).containsEntry("features.isNewBeneficiary", false).containsEntry("features.isNewDevice", false);
    }

    @Test
    @DisplayName("todo valor tem o tipo do catálogo e toda chave existe nele")
    void tiposDoCatalogo() {
        var snapshot = evaluate(new AccountFeatures(), tx(T, 100, "acc_tok_dest0001", "dev_aaaaaaaa"));
        var catalog = FeatureCatalog.standardCatalog().features();

        snapshot.values().forEach((name, value) -> {
            assertThat(catalog).as(name).containsKey(name);
            switch (catalog.get(name)) {
                case INT -> assertThat(value).as(name).isInstanceOf(Long.class);
                case BOOL -> assertThat(value).as(name).isInstanceOf(Boolean.class);
                case STRING -> assertThat(value).as(name).isInstanceOf(String.class);
            }
        });
        assertThat(snapshot.values().keySet()).filteredOn(k -> k.startsWith("features.")).hasSize(12);
        assertThat(FeatureType.values()).hasSize(3);
    }

    @Test
    @DisplayName("o mapa devolvido é imutável")
    void imutavel() {
        var snapshot = evaluate(new AccountFeatures(), tx(T, 100, "acc_tok_dest0001", "dev_aaaaaaaa"));

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> snapshot.values().put("x", 1L))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
