package br.com.motorfraude.engine.application;

import static br.com.motorfraude.engine.TestData.ACCOUNT;
import static br.com.motorfraude.engine.TestData.activity;
import static br.com.motorfraude.engine.TestData.profile;
import static br.com.motorfraude.engine.TestData.tx;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import br.com.motorfraude.contracts.ContextCompleteness;
import br.com.motorfraude.contracts.ContractAssertions;
import br.com.motorfraude.contracts.EvaluateTransaction;
import br.com.motorfraude.contracts.Segment;
import br.com.motorfraude.contracts.Severity;
import br.com.motorfraude.engine.InitialRules;
import br.com.motorfraude.engine.fakes.FixedRuleSetProvider;
import br.com.motorfraude.engine.fakes.InMemoryBlocklist;
import br.com.motorfraude.engine.fakes.InMemoryDedupStore;
import br.com.motorfraude.engine.fakes.InMemoryFeatureStore;
import br.com.motorfraude.engine.fakes.InMemoryProfileReader;
import br.com.motorfraude.rules.kernel.model.RuleStatus;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class EvaluateTransactionUseCaseTest {

    private static final Instant THREE_AM_SP = Instant.parse("2026-11-10T06:00:30Z");
    private static final Instant RECEIVED = THREE_AM_SP.plusMillis(40);
    private static final Instant DECIDED = THREE_AM_SP.plusMillis(120);
    private static final String TRACE = "4bf92f3577b34da6a3ce929d0e0e4736";

    private final InMemoryDedupStore dedup = new InMemoryDedupStore();
    private final InMemoryFeatureStore featureStore = new InMemoryFeatureStore();
    private final InMemoryProfileReader profiles = new InMemoryProfileReader();
    private final InMemoryBlocklist blocklist = new InMemoryBlocklist();
    private final FixedRuleSetProvider ruleSets = new FixedRuleSetProvider(InitialRules.compiled(7));
    private EvaluateTransactionUseCase useCase;

    @BeforeEach
    void setUp() {
        useCase = new EvaluateTransactionUseCase(dedup, featureStore, profiles, blocklist, ruleSets, Clock.fixed(DECIDED, ZoneOffset.UTC));
    }

    private void seedThirtyDaysOfHistory() {
        var features = featureStore.load(ACCOUNT);
        for (int day = 1; day <= 29; day++) {
            features.record(activity(tx("seed-%02d-abcdef".formatted(day), THREE_AM_SP.minus(Duration.ofDays(day)), 20_000, "acc_tok_dest0001",
                    "dev_aaaaaaaa")));
        }
        featureStore.save(ACCOUNT, features);
    }

    private Outcome run(EvaluateTransaction transaction) {
        return useCase.execute(transaction, RECEIVED, TRACE);
    }

    @Test
    @DisplayName("sem regra disparada: NO_ALERT, nenhum alerta, e a decisão passa no contrato")
    void semRegra() {
        seedThirtyDaysOfHistory();
        profiles.put(profile(Segment.PF, THREE_AM_SP.minus(Duration.ofDays(400)), null));

        var outcome = run(tx("tx-0001-abcdef", THREE_AM_SP.plus(Duration.ofHours(10)), 5_000, "acc_tok_dest0001", "dev_aaaaaaaa"));

        assertThat(outcome.duplicate()).isFalse();
        assertThat(outcome.late()).isFalse();
        var decision = outcome.decision();
        assertThat(decision.severity()).isEqualTo(Severity.NONE);
        assertThat(decision.alertMessage()).isEmpty();
        ContractAssertions.assertProduces("fraud-decision.v1", decision.decisionMessage());
    }

    @Test
    @DisplayName("valor alto de madrugada acima da média: R04 + R05 somam 50 pontos e dão MEDIUM, com alerta")
    void medium() {
        seedThirtyDaysOfHistory();
        profiles.put(profile(Segment.PF, THREE_AM_SP.minus(Duration.ofDays(400)), null));

        var decision = run(tx("tx-0002-abcdef", THREE_AM_SP, 150_000, "acc_tok_dest0001", "dev_aaaaaaaa")).decision();

        assertThat(decision.decisionMessage().triggeredRules()).extracting("ruleId").containsExactlyInAnyOrder("R04", "R05");
        assertThat(decision.decisionMessage().score()).isEqualTo(50);
        assertThat(decision.severity()).isEqualTo(Severity.MEDIUM);
        var alert = decision.alertMessage().orElseThrow();
        assertThat(alert.ruleSetVersion()).isEqualTo(7);
        assertThat(alert.traceId()).isEqualTo(TRACE);
        ContractAssertions.assertProduces("fraud-alert.v1", alert);
        ContractAssertions.assertProduces("fraud-decision.v1", decision.decisionMessage());
    }

    @Test
    @DisplayName("beneficiário em lista de bloqueio: R10 sozinha dá 70 pontos, HIGH")
    void blocklist() {
        profiles.put(profile(Segment.PF, THREE_AM_SP.minus(Duration.ofDays(400)), null));
        blocklist.flagAccount("acc_tok_dest0666");

        var decision = run(tx("tx-0003-abcdef", THREE_AM_SP.plus(Duration.ofHours(10)), 5_000, "acc_tok_dest0666", "dev_aaaaaaaa")).decision();

        assertThat(decision.severity()).isEqualTo(Severity.HIGH);
        assertThat(decision.decisionMessage().triggeredRules()).extracting("ruleId").contains("R10");
        assertThat(decision.decisionMessage().features()).containsEntry("lists.counterpartyFlagged", true);
    }

    @Test
    @DisplayName("sequestro de conta (credencial trocada há 2 h, dispositivo e beneficiário novos, valor alto): HIGH")
    void sequestro() {
        profiles.put(profile(Segment.PF, THREE_AM_SP.minus(Duration.ofDays(400)), THREE_AM_SP.minus(Duration.ofHours(2))));

        var decision = run(tx("tx-0004-abcdef", THREE_AM_SP.plus(Duration.ofHours(10)), 250_000, "acc_tok_dest0777", "dev_novo0001")).decision();

        assertThat(decision.severity()).isEqualTo(Severity.HIGH);
        assertThat(decision.decisionMessage().contextCompleteness()).isEqualTo(ContextCompleteness.FULL);
        assertThat(decision.decisionMessage().triggeredRules()).extracting("ruleId").contains("R01", "R03", "R12");
    }

    @Test
    @DisplayName("perfil ausente: PARTIAL, e as regras que dependem do perfil (R01, R12) não disparam")
    void perfilAusente() {
        var decision = run(tx("tx-0005-abcdef", THREE_AM_SP.plus(Duration.ofHours(10)), 250_000, "acc_tok_dest0777", "dev_novo0001")).decision();

        assertThat(decision.decisionMessage().contextCompleteness()).isEqualTo(ContextCompleteness.PARTIAL);
        assertThat(decision.decisionMessage().triggeredRules()).extracting("ruleId").doesNotContain("R01", "R12");
        assertThat(decision.decisionMessage().triggeredRules()).extracting("ruleId").contains("R03");
        assertThat(decision.decisionMessage().features()).containsEntry("profile.segment", "UNKNOWN");
    }

    @Test
    @DisplayName("regra em sombra não soma pontos e não gera alerta, mas aparece em shadowHits")
    void sombra() {
        ruleSets.set(InitialRules.compiled(8, Map.of("R10", RuleStatus.SHADOW)));
        profiles.put(profile(Segment.PF, THREE_AM_SP.minus(Duration.ofDays(400)), null));
        blocklist.flagAccount("acc_tok_dest0666");

        var decision = run(tx("tx-0006-abcdef", THREE_AM_SP.plus(Duration.ofHours(10)), 5_000, "acc_tok_dest0666", "dev_aaaaaaaa")).decision();

        assertThat(decision.decisionMessage().score()).isZero();
        assertThat(decision.alertMessage()).isEmpty();
        assertThat(decision.decisionMessage().shadowHits()).extracting("ruleId").containsExactly("R10");
        assertThat(decision.decisionMessage().ruleSetVersion()).isEqualTo(8);
    }

    @Test
    @DisplayName("transação repetida é duplicata: não gera decisão e não conta duas vezes nas features")
    void duplicata() {
        profiles.put(profile(Segment.PF, THREE_AM_SP.minus(Duration.ofDays(400)), null));
        var transaction = tx("tx-0007-abcdef", THREE_AM_SP, 5_000, "acc_tok_dest0001", "dev_aaaaaaaa");

        var first = run(transaction);
        var second = run(transaction);

        assertThat(first.duplicate()).isFalse();
        assertThat(second.duplicate()).isTrue();
        assertThat(second.decision()).isNull();
        assertThat(featureStore.load(ACCOUNT).count(THREE_AM_SP, br.com.motorfraude.engine.domain.features.Window.H24)).isEqualTo(1);
    }

    @Test
    @DisplayName("marca o transactionId no dedup com o relógio do serviço")
    void marcaDedup() {
        run(tx("tx-0008-abcdef", THREE_AM_SP, 5_000, "acc_tok_dest0001", "dev_aaaaaaaa"));

        assertThat(dedup.markedAt("tx-0008-abcdef")).isEqualTo(DECIDED);
    }

    @Test
    @DisplayName("grava as features da conta; a mesma conta e o mesmo beneficiário na próxima já não são novos")
    void gravaFeatures() {
        profiles.put(profile(Segment.PF, THREE_AM_SP.minus(Duration.ofDays(400)), null));

        var first = run(tx("tx-0009-abcdef", THREE_AM_SP, 5_000, "acc_tok_dest0001", "dev_aaaaaaaa")).decision();
        var second = run(tx("tx-0010-abcdef", THREE_AM_SP.plusSeconds(10), 5_000, "acc_tok_dest0001", "dev_aaaaaaaa")).decision();

        assertThat(first.decisionMessage().features()).containsEntry("features.isNewBeneficiary", true).containsEntry("features.count1m", 1L);
        assertThat(second.decisionMessage().features()).containsEntry("features.isNewBeneficiary", false).containsEntry("features.count1m", 2L);
    }

    @Test
    @DisplayName("os instantes da decisão: recebimento vindo do cabeçalho e decisão pelo relógio")
    void instantes() {
        var decision = run(tx("tx-0011-abcdef", THREE_AM_SP, 5_000, "acc_tok_dest0001", "dev_aaaaaaaa")).decision();

        assertThat(decision.decisionMessage().eventReceivedAt()).isEqualTo(RECEIVED);
        assertThat(decision.decisionMessage().decidedAt()).isEqualTo(DECIDED);
    }

    @Test
    @DisplayName("sem conjunto de regras carregado não há como decidir: erro explícito, e nada é marcado nem gravado")
    void semConjunto() {
        ruleSets.set(null);
        var transaction = tx("tx-0012-abcdef", THREE_AM_SP, 5_000, "acc_tok_dest0001", "dev_aaaaaaaa");

        assertThatThrownBy(() -> run(transaction)).isInstanceOf(IllegalStateException.class).hasMessageContaining("conjunto de regras");
        assertThat(dedup.seen("tx-0012-abcdef")).isFalse();
    }

    @Test
    @DisplayName("evento com até 2 min de atraso entra no bucket do seu occurredAt e não é marcado como atrasado")
    void atrasoTolerado() {
        profiles.put(profile(Segment.PF, THREE_AM_SP.minus(Duration.ofDays(400)), null));
        var occurredAt = RECEIVED.minusSeconds(90);

        var outcome = run(tx("tx-0013-abcdef", occurredAt, 5_000, "acc_tok_dest0001", "dev_aaaaaaaa"));

        assertThat(outcome.late()).isFalse();
        assertThat(featureStore.load(ACCOUNT).count(occurredAt, br.com.motorfraude.engine.domain.features.Window.M1)).isEqualTo(1);
    }

    @Test
    @DisplayName("evento com mais de 2 min de atraso entra no bucket do horário de chegada e é marcado como atrasado")
    void atrasoDemais() {
        profiles.put(profile(Segment.PF, THREE_AM_SP.minus(Duration.ofDays(400)), null));
        var occurredAt = RECEIVED.minus(Duration.ofMinutes(10));

        var outcome = run(tx("tx-0014-abcdef", occurredAt, 5_000, "acc_tok_dest0001", "dev_aaaaaaaa"));

        assertThat(outcome.late()).isTrue();
        var features = featureStore.load(ACCOUNT);
        assertThat(features.count(RECEIVED, br.com.motorfraude.engine.domain.features.Window.M1)).isEqualTo(1);
        assertThat(features.count(occurredAt, br.com.motorfraude.engine.domain.features.Window.M1)).isZero();
        assertThat(outcome.decision().decisionMessage().features()).containsEntry("features.count1m", 1L);
    }
}
