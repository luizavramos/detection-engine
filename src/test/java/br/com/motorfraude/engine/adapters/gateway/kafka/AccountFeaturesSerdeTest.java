package br.com.motorfraude.engine.adapters.gateway.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import br.com.motorfraude.engine.domain.features.AccountFeatures;
import br.com.motorfraude.engine.domain.features.Activity;
import br.com.motorfraude.engine.domain.features.BucketRing;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AccountFeaturesSerdeTest {

    private static final Instant T = Instant.parse("2026-11-10T15:00:30Z");

    static void assertSameState(AccountFeatures actual, AccountFeatures expected) {
        assertSameRing(actual.minutes(), expected.minutes());
        assertSameRing(actual.hours(), expected.hours());
        assertSameRing(actual.days(), expected.days());
        assertThat(actual.beneficiaries().tokens()).containsExactlyElementsOf(expected.beneficiaries().tokens());
        assertThat(actual.devices().tokens()).containsExactlyElementsOf(expected.devices().tokens());
        for (var token : expected.beneficiaries().tokens()) {
            assertThat(actual.beneficiaries().lastSeen(token)).isEqualTo(expected.beneficiaries().lastSeen(token));
        }
        for (var token : expected.devices().tokens()) {
            assertThat(actual.devices().lastSeen(token)).isEqualTo(expected.devices().lastSeen(token));
        }
    }

    private static void assertSameRing(BucketRing actual, BucketRing expected) {
        assertThat(actual.size()).isEqualTo(expected.size());
        for (int i = 0; i < expected.size(); i++) {
            assertThat(actual.epochAt(i)).as("epoch %d", i).isEqualTo(expected.epochAt(i));
            if (expected.epochAt(i) != BucketRing.EMPTY) {
                assertThat(actual.countAt(i)).isEqualTo(expected.countAt(i));
                assertThat(actual.sumAt(i)).isEqualTo(expected.sumAt(i));
                assertThat(actual.maxAt(i)).isEqualTo(expected.maxAt(i));
            }
        }
    }

    @Provide
    Arbitrary<List<Activity>> activities() {
        var activity = Combinators.combine(
                Arbitraries.integers().between(0, 200 * 24 * 60),
                Arbitraries.integers().between(1, 80),
                Arbitraries.integers().between(0, 20),
                Arbitraries.longs().between(0, 9_000_000_000L))
                .as((minutesBack, beneficiary, device, amount) -> new Activity(
                        "acc_tok_%08d".formatted(beneficiary), device == 0 ? null : "dev_%08d".formatted(device), amount,
                        T.minus(Duration.ofMinutes(minutesBack))));
        return activity.list().ofMaxSize(150);
    }

    @Property
    void idaEVoltaPreservaOEstado(@ForAll("activities") List<Activity> activities) {
        var features = new AccountFeatures();
        activities.forEach(features::record);

        var back = AccountFeaturesSerde.deserialize(AccountFeaturesSerde.serialize(features));

        assertSameState(back, features);
    }

    @Property
    void bytesSaoDeterministicos(@ForAll("activities") List<Activity> activities) {
        var features = new AccountFeatures();
        activities.forEach(features::record);

        assertThat(AccountFeaturesSerde.serialize(features)).isEqualTo(AccountFeaturesSerde.serialize(features));
        var again = AccountFeaturesSerde.deserialize(AccountFeaturesSerde.serialize(features));
        assertThat(AccountFeaturesSerde.serialize(again)).isEqualTo(AccountFeaturesSerde.serialize(features));
    }

    @Test
    @DisplayName("conta vazia ocupa só alguns bytes")
    void vazia() {
        var bytes = AccountFeaturesSerde.serialize(new AccountFeatures());

        assertThat(bytes.length).isLessThan(16);
        assertSameState(AccountFeaturesSerde.deserialize(bytes), new AccountFeatures());
    }

    @Test
    @DisplayName("SC-004: conta cheia (todos os buckets, 50 beneficiários e 10 dispositivos) ocupa no máximo 5 KB")
    void contaCheia() {
        var features = new AccountFeatures();
        for (int minute = 0; minute < 60; minute++) {
            features.record(new Activity("acc_tok_%08d".formatted(minute), null, 123_456_789L + minute, T.minus(Duration.ofMinutes(minute))));
        }
        for (int hour = 1; hour < 24; hour++) {
            features.record(new Activity("acc_tok_%08d".formatted(60 + hour), null, 987_654_321L, T.minus(Duration.ofHours(hour + 1))));
        }
        for (int day = 2; day < 91; day++) {
            features.record(new Activity("acc_tok_%08d".formatted(100 + day), "dev_%08d".formatted(day % 12),
                    5_000_000_000L, T.minus(Duration.ofDays(day))));
        }

        var bytes = AccountFeaturesSerde.serialize(features);

        assertThat(features.beneficiaries().size()).isEqualTo(50);
        assertThat(features.devices().size()).isEqualTo(10);
        assertThat(bytes.length).as("tamanho em bytes").isLessThanOrEqualTo(5 * 1024);
        assertSameState(AccountFeaturesSerde.deserialize(bytes), features);
    }

    @Test
    @DisplayName("versão desconhecida, vazio e dados truncados são rejeitados com erro claro")
    void invalidos() {
        var bytes = AccountFeaturesSerde.serialize(new AccountFeatures());
        var wrongVersion = bytes.clone();
        wrongVersion[0] = 99;

        assertThatThrownBy(() -> AccountFeaturesSerde.deserialize(wrongVersion)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("versão");
        assertThatThrownBy(() -> AccountFeaturesSerde.deserialize(new byte[0])).isInstanceOf(IllegalArgumentException.class);

        var features = new AccountFeatures();
        features.record(new Activity("acc_tok_00000001", "dev_00000001", 100, T));
        var full = AccountFeaturesSerde.serialize(features);
        var truncated = java.util.Arrays.copyOf(full, full.length - 3);
        assertThatThrownBy(() -> AccountFeaturesSerde.deserialize(truncated)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("o serde do Kafka devolve nulo para nulo (lápide) e faz a ida e volta")
    void serdeDoKafka() {
        var serde = AccountFeaturesSerde.kafkaSerde();
        var features = new AccountFeatures();
        features.record(new Activity("acc_tok_00000001", "dev_00000001", 100, T));

        assertThat(serde.serializer().serialize("t", null)).isNull();
        assertThat(serde.deserializer().deserialize("t", null)).isNull();
        assertSameState(serde.deserializer().deserialize("t", serde.serializer().serialize("t", features)), features);
    }
}
