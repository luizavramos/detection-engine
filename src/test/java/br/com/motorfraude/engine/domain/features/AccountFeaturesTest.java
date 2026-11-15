package br.com.motorfraude.engine.domain.features;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AccountFeaturesTest {

    private static final Instant T = Instant.parse("2026-11-10T15:00:30Z");

    private static Activity at(Instant when, long amount, String counterparty, String device) {
        return new Activity(counterparty, device, amount, when);
    }

    private static Activity at(Instant when, long amount) {
        return at(when, amount, "acc_tok_dest0001", "dev_aaaaaaaa");
    }

    @Test
    @DisplayName("conta nova: baseline zerada e tudo é novo")
    void contaNova() {
        var features = new AccountFeatures();
        var baseline = features.baselineFor(at(T, 100));

        assertThat(baseline.avgAmount30d()).isZero();
        assertThat(baseline.maxAmount90d()).isZero();
        assertThat(baseline.newBeneficiary()).isTrue();
        assertThat(baseline.newDevice()).isTrue();
        assertThat(features.count(T, Window.M1)).isZero();
    }

    @Test
    @DisplayName("H3.1: 6 transações em 4 minutos dão count5m = 6, com a atual dentro")
    void seisEmQuatroMinutos() {
        var features = new AccountFeatures();
        for (int i = 0; i < 6; i++) {
            features.record(at(T.plusSeconds(i * 48L), 1000));
        }
        var now = T.plusSeconds(5 * 48L);

        assertThat(features.count(now, Window.M5)).isEqualTo(6);
        assertThat(features.count(now, Window.M1)).isLessThan(6);
        assertThat(features.count(now, Window.H1)).isEqualTo(6);
    }

    @Test
    @DisplayName("cada janela de contagem enxerga só o que cabe nela")
    void janelas() {
        var features = new AccountFeatures();
        features.record(at(T.minus(Duration.ofDays(29)), 100));
        features.record(at(T.minus(Duration.ofDays(31)), 100));
        features.record(at(T.minus(Duration.ofHours(23)), 100));
        features.record(at(T.minus(Duration.ofMinutes(59)), 100));
        features.record(at(T.minus(Duration.ofMinutes(4)), 100));
        features.record(at(T, 100));

        assertThat(features.count(T, Window.M1)).isEqualTo(1);
        assertThat(features.count(T, Window.M5)).isEqualTo(2);
        assertThat(features.count(T, Window.H1)).isEqualTo(3);
        assertThat(features.count(T, Window.H24)).isEqualTo(4);
        assertThat(features.count(T, Window.D30)).isEqualTo(5);
    }

    @Test
    @DisplayName("soma em 1 h e em 24 h, em centavos")
    void somas() {
        var features = new AccountFeatures();
        features.record(at(T.minus(Duration.ofHours(10)), 5000));
        features.record(at(T.minus(Duration.ofMinutes(30)), 2000));
        features.record(at(T, 300));

        assertThat(features.sum(T, Window.H1)).isEqualTo(2300);
        assertThat(features.sum(T, Window.H24)).isEqualTo(7300);
    }

    @Test
    @DisplayName("H3.2: média e máximo são a referência anterior à transação atual (20000 de média, 200000 chegando)")
    void baselineDeValor() {
        var features = new AccountFeatures();
        for (int day = 1; day <= 30; day++) {
            features.record(at(T.minus(Duration.ofDays(day)).plusSeconds(60), 20_000));
        }
        features.record(at(T.minus(Duration.ofDays(60)), 90_000));
        var current = at(T, 200_000);

        var baseline = features.baselineFor(current);
        features.record(current);

        assertThat(baseline.avgAmount30d()).isEqualTo(20_000);
        assertThat(baseline.maxAmount90d()).isEqualTo(90_000);
        assertThat(features.sum(T, Window.H24)).isEqualTo(200_000);
    }

    @Test
    @DisplayName("H3.3: beneficiário novo é detectado antes do registro e deixa de ser novo na próxima")
    void beneficiarioNovo() {
        var features = new AccountFeatures();
        var first = at(T, 100, "acc_tok_dest0001", "dev_aaaaaaaa");

        assertThat(features.baselineFor(first).newBeneficiary()).isTrue();
        features.record(first);

        var second = at(T.plusSeconds(10), 100, "acc_tok_dest0001", "dev_aaaaaaaa");
        assertThat(features.baselineFor(second).newBeneficiary()).isFalse();
        assertThat(features.baselineFor(second).newDevice()).isFalse();
        assertThat(features.baselineFor(at(T, 100, "acc_tok_dest0002", "dev_bbbbbbbb")).newBeneficiary()).isTrue();
        assertThat(features.baselineFor(at(T, 100, "acc_tok_dest0002", "dev_bbbbbbbb")).newDevice()).isTrue();
    }

    @Test
    @DisplayName("sem dispositivo informado, o dispositivo não é considerado novo")
    void semDispositivo() {
        var features = new AccountFeatures();
        var tx = at(T, 100, "acc_tok_dest0001", null);

        assertThat(features.baselineFor(tx).newDevice()).isFalse();
        features.record(tx);
        assertThat(features.deviceCount()).isZero();
    }

    @Test
    @DisplayName("beneficiários distintos nas últimas 24 h")
    void beneficiariosDistintos() {
        var features = new AccountFeatures();
        features.record(at(T.minus(Duration.ofHours(30)), 100, "acc_tok_dest0001", "dev_aaaaaaaa"));
        features.record(at(T.minus(Duration.ofHours(5)), 100, "acc_tok_dest0002", "dev_aaaaaaaa"));
        features.record(at(T.minus(Duration.ofHours(1)), 100, "acc_tok_dest0002", "dev_aaaaaaaa"));
        features.record(at(T, 100, "acc_tok_dest0003", "dev_aaaaaaaa"));

        assertThat(features.distinctBeneficiaries(T, Duration.ofHours(24))).isEqualTo(2);
    }

    @Test
    @DisplayName("H3.4: evento fora de ordem (até 2 min) entra no bucket do seu instante")
    void foraDeOrdem() {
        var features = new AccountFeatures();
        features.record(at(T.plusSeconds(180), 100));

        var late = at(T.plusSeconds(60), 500);
        features.record(late);

        assertThat(features.count(T.plusSeconds(60), Window.M1)).isEqualTo(1);
        assertThat(features.sum(T.plusSeconds(60), Window.M1)).isEqualTo(500);
        assertThat(features.count(T.plusSeconds(180), Window.M5)).isEqualTo(2);
    }

    @Test
    @DisplayName("evento mais velho que o anel de minutos fica de fora das janelas curtas, mas entra nas longas")
    void velhoDemais() {
        var features = new AccountFeatures();
        features.record(at(T, 100));

        features.record(at(T.minus(Duration.ofHours(3)), 777));
        assertThat(features.count(T, Window.H1)).isEqualTo(1);
        assertThat(features.count(T, Window.H24)).isEqualTo(2);
        assertThat(features.sum(T, Window.H24)).isEqualTo(877);
    }
}
