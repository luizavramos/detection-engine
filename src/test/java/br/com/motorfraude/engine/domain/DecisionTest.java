package br.com.motorfraude.engine.domain;

import static br.com.motorfraude.engine.TestData.snapshot;
import static br.com.motorfraude.engine.TestData.tx;
import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.contracts.ContextCompleteness;
import br.com.motorfraude.contracts.ContractAssertions;
import br.com.motorfraude.contracts.Severity;
import br.com.motorfraude.rules.kernel.evaluation.Evaluation;
import br.com.motorfraude.rules.kernel.evaluation.RuleError;
import br.com.motorfraude.rules.kernel.evaluation.RuleHit;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DecisionTest {

    private static final Instant RECEIVED = Instant.parse("2026-11-10T15:00:30.100Z");
    private static final Instant DECIDED = Instant.parse("2026-11-10T15:00:30.180Z");
    private static final String TRACE = "4bf92f3577b34da6a3ce929d0e0e4736";

    private static Evaluation evaluation(int score, br.com.motorfraude.rules.kernel.evaluation.Severity severity, List<RuleHit> hits,
            List<RuleHit> shadow) {
        return new Evaluation(7, score, severity, hits, shadow, List.of());
    }

    private static Decision decide(Evaluation evaluation, ContextCompleteness completeness, String trace) {
        var transaction = tx("tx-0001-abcdef", 150_000);
        return Decision.from(transaction, evaluation, snapshot(transaction), completeness, RECEIVED, DECIDED, trace);
    }

    private static final RuleHit R04 = new RuleHit("R04", 1, 35, "Valor acima de 5x a média");
    private static final RuleHit R05 = new RuleHit("R05", 1, 15, "Madrugada");
    private static final RuleHit R10 = new RuleHit("R10", 1, 70, "Beneficiário em lista de bloqueio");

    @Test
    @DisplayName("sem regra disparada: NO_ALERT, severidade NONE e nenhum alerta")
    void semRegra() {
        var decision = decide(evaluation(0, br.com.motorfraude.rules.kernel.evaluation.Severity.NONE, List.of(), List.of()),
                ContextCompleteness.FULL, TRACE);

        assertThat(decision.severity()).isEqualTo(Severity.NONE);
        assertThat(decision.isAlert()).isFalse();
        assertThat(decision.alertMessage()).isEmpty();
        var message = decision.decisionMessage();
        assertThat(message.decision()).isEqualTo(br.com.motorfraude.contracts.Decision.NO_ALERT);
        assertThat(message.score()).isZero();
        ContractAssertions.assertProduces("fraud-decision.v1", message);
    }

    @Test
    @DisplayName("55 pontos nas faixas do conjunto: MEDIUM com alerta; o alerta leva as regras, a versão e o trace")
    void medium() {
        var decision = decide(evaluation(55, br.com.motorfraude.rules.kernel.evaluation.Severity.MEDIUM, List.of(R04, R05), List.of()),
                ContextCompleteness.FULL, TRACE);

        assertThat(decision.severity()).isEqualTo(Severity.MEDIUM);
        var alert = decision.alertMessage().orElseThrow();
        assertThat(alert.severity()).isEqualTo(Severity.MEDIUM);
        assertThat(alert.score()).isEqualTo(55);
        assertThat(alert.triggeredRules()).extracting("ruleId").containsExactly("R04", "R05");
        assertThat(alert.ruleSetVersion()).isEqualTo(7);
        assertThat(alert.traceId()).isEqualTo(TRACE);
        assertThat(alert.eventReceivedAt()).isEqualTo(RECEIVED);
        assertThat(alert.detectedAt()).isEqualTo(DECIDED);
        ContractAssertions.assertProduces("fraud-alert.v1", alert);
        ContractAssertions.assertProduces("fraud-decision.v1", decision.decisionMessage());
    }

    @Test
    @DisplayName("70 pontos ou mais: HIGH")
    void high() {
        var decision = decide(evaluation(70, br.com.motorfraude.rules.kernel.evaluation.Severity.HIGH, List.of(R10), List.of()),
                ContextCompleteness.FULL, TRACE);

        assertThat(decision.severity()).isEqualTo(Severity.HIGH);
        assertThat(decision.decisionMessage().decision()).isEqualTo(br.com.motorfraude.contracts.Decision.ALERT);
        assertThat(decision.alertMessage().orElseThrow().severity()).isEqualTo(Severity.HIGH);
    }

    @Test
    @DisplayName("regras em sombra aparecem na decisão, mas não no alerta nem na pontuação")
    void sombra() {
        var decision = decide(evaluation(0, br.com.motorfraude.rules.kernel.evaluation.Severity.NONE, List.of(), List.of(R10)),
                ContextCompleteness.FULL, TRACE);

        var message = decision.decisionMessage();
        assertThat(message.shadowHits()).extracting("ruleId").containsExactly("R10");
        assertThat(message.triggeredRules()).isEmpty();
        assertThat(message.score()).isZero();
        assertThat(decision.alertMessage()).isEmpty();
    }

    @Test
    @DisplayName("perfil ausente: PARTIAL na decisão e no alerta")
    void parcial() {
        var decision = decide(evaluation(70, br.com.motorfraude.rules.kernel.evaluation.Severity.HIGH, List.of(R10), List.of()),
                ContextCompleteness.PARTIAL, TRACE);

        assertThat(decision.decisionMessage().contextCompleteness()).isEqualTo(ContextCompleteness.PARTIAL);
        assertThat(decision.alertMessage().orElseThrow().contextCompleteness()).isEqualTo(ContextCompleteness.PARTIAL);
    }

    @Test
    @DisplayName("alertId e decisionId são UUIDv5 estáveis do transactionId, e diferentes entre si")
    void identificadoresEstaveis() {
        var evaluation = evaluation(70, br.com.motorfraude.rules.kernel.evaluation.Severity.HIGH, List.of(R10), List.of());
        var first = decide(evaluation, ContextCompleteness.FULL, TRACE);
        var second = decide(evaluation, ContextCompleteness.FULL, TRACE);

        var alertId = first.alertMessage().orElseThrow().alertId();
        assertThat(alertId).isEqualTo(second.alertMessage().orElseThrow().alertId())
                .isEqualTo(Uuids.v5(Uuids.NAMESPACE, "tx-0001-abcdef").toString());
        assertThat(first.decisionMessage().decisionId()).isEqualTo(second.decisionMessage().decisionId()).isNotEqualTo(alertId);
    }

    @Test
    @DisplayName("traceId inválido ou ausente vira um id derivado do transactionId, estável e com 32 hexadecimais")
    void traceDerivado() {
        var evaluation = evaluation(70, br.com.motorfraude.rules.kernel.evaluation.Severity.HIGH, List.of(R10), List.of());

        var semTrace = decide(evaluation, ContextCompleteness.FULL, null).alertMessage().orElseThrow().traceId();
        var invalido = decide(evaluation, ContextCompleteness.FULL, "não-é-trace").alertMessage().orElseThrow().traceId();

        assertThat(semTrace).matches("[0-9a-f]{32}").isEqualTo(invalido);
        assertThat(semTrace).isNotEqualTo("00000000000000000000000000000000");
    }

    @Test
    @DisplayName("a decisão leva o instantâneo das features e os erros de regra ficam disponíveis para métrica")
    void featuresEErros() {
        var errors = List.of(new RuleError("R09", 1, "sem atributo"));
        var withErrors = new Evaluation(7, 0, br.com.motorfraude.rules.kernel.evaluation.Severity.NONE, List.of(), List.of(), errors);
        var decision = decide(withErrors, ContextCompleteness.FULL, TRACE);

        assertThat(decision.decisionMessage().features()).containsEntry("tx.amount", 150_000L).containsKey("features.count5m");
        assertThat(decision.ruleErrors()).hasSize(1);
    }
}
