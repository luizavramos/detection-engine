package br.com.motorfraude.engine.adapters.gateway.micrometer;

import static br.com.motorfraude.engine.TestData.snapshot;
import static br.com.motorfraude.engine.TestData.tx;
import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.contracts.ContextCompleteness;
import br.com.motorfraude.engine.domain.Decision;
import br.com.motorfraude.rules.kernel.evaluation.Evaluation;
import br.com.motorfraude.rules.kernel.evaluation.RuleError;
import br.com.motorfraude.rules.kernel.evaluation.RuleHit;
import br.com.motorfraude.rules.kernel.evaluation.Severity;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MicrometerEngineMetricsTest {

    private static final Instant NOW = Instant.parse("2026-11-15T12:00:20Z");

    private final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    private final MicrometerEngineMetrics metrics = new MicrometerEngineMetrics(registry, Clock.fixed(NOW, ZoneOffset.UTC));

    private String scrape() {
        return registry.scrape();
    }

    private static Decision decision(Severity severity, ContextCompleteness completeness, List<RuleHit> active, List<RuleHit> shadow,
            List<RuleError> errors) {
        var transaction = tx("tx-0001-abcdef", 150_000);
        var evaluation = new Evaluation(7, severity == Severity.NONE ? 0 : 80, severity, active, shadow, errors);
        return Decision.from(transaction, evaluation, snapshot(transaction), completeness, NOW.minusMillis(120), NOW, null);
    }

    private static final RuleHit R10 = new RuleHit("R10", 1, 70, "lista de bloqueio");
    private static final RuleHit R07 = new RuleHit("R07", 1, 25, "rajada");

    @Test
    @DisplayName("todas as séries existem em zero desde a partida, para o painel não ficar vazio")
    void zeradasNaPartida() {
        var text = scrape();

        assertThat(text).contains("engine_decisions_total{severity=\"NONE\"} 0.0", "engine_decisions_total{severity=\"MEDIUM\"} 0.0",
                "engine_decisions_total{severity=\"HIGH\"} 0.0", "engine_duplicates_dropped_total 0.0", "engine_context_partial_total 0.0",
                "engine_late_events_total 0.0", "engine_dlq_total 0.0", "engine_ruleset_version 0.0");
    }

    @Test
    @DisplayName("conta a decisão por severidade e os acertos por regra e modo (ativa ou sombra)")
    void decisoes() {
        metrics.decided(decision(Severity.HIGH, ContextCompleteness.FULL, List.of(R10), List.of(R07), List.of()), Duration.ofMillis(120));
        metrics.decided(decision(Severity.NONE, ContextCompleteness.FULL, List.of(), List.of(), List.of()), Duration.ofMillis(30));

        var text = scrape();
        assertThat(text).contains("engine_decisions_total{severity=\"HIGH\"} 1.0", "engine_decisions_total{severity=\"NONE\"} 1.0",
                "engine_rule_hits_total{mode=\"active\",rule_id=\"R10\"} 1.0", "engine_rule_hits_total{mode=\"shadow\",rule_id=\"R07\"} 1.0");
    }

    @Test
    @DisplayName("contexto parcial e erro de regra são contados")
    void parcialEErros() {
        metrics.decided(decision(Severity.NONE, ContextCompleteness.PARTIAL, List.of(), List.of(), List.of(new RuleError("R09", 1, "x"))),
                Duration.ofMillis(10));

        assertThat(scrape()).contains("engine_context_partial_total 1.0", "engine_rule_errors_total{rule_id=\"R09\"} 1.0");
    }

    @Test
    @DisplayName("a latência fim a fim é um histograma com o limite do SLO (0,5 s) como fronteira de bucket")
    void latenciaFimAFim() {
        metrics.decided(decision(Severity.NONE, ContextCompleteness.FULL, List.of(), List.of(), List.of()), Duration.ofMillis(120));
        metrics.decided(decision(Severity.NONE, ContextCompleteness.FULL, List.of(), List.of(), List.of()), Duration.ofMillis(800));

        var text = scrape();
        assertThat(text).contains("engine_e2e_latency_seconds_count 2", "engine_e2e_latency_seconds_bucket{le=\"0.5\"} 1",
                "engine_e2e_latency_seconds_bucket{le=\"0.1\"} 0", "engine_e2e_latency_seconds_bucket{le=\"1.0\"} 2");
    }

    @Test
    @DisplayName("latência negativa (relógios diferentes) vira zero em vez de quebrar o histograma")
    void latenciaNegativa() {
        metrics.decided(decision(Severity.NONE, ContextCompleteness.FULL, List.of(), List.of(), List.of()), Duration.ofMillis(-50));

        assertThat(scrape()).contains("engine_e2e_latency_seconds_count 1");
    }

    @Test
    @DisplayName("duplicatas, eventos atrasados e DLQ têm contadores próprios")
    void contadores() {
        metrics.duplicateDropped();
        metrics.duplicateDropped();
        metrics.lateEvent();
        metrics.dlq();

        assertThat(scrape()).contains("engine_duplicates_dropped_total 2.0", "engine_late_events_total 1.0", "engine_dlq_total 1.0");
    }

    @Test
    @DisplayName("conjunto de regras: versão em uso, idade desde a publicação e falhas por motivo")
    void conjuntoDeRegras() {
        metrics.loaded(42, NOW.minusSeconds(90));
        metrics.failed("compile");
        metrics.failed("compile");
        metrics.failed("kafka");

        assertThat(scrape()).contains("engine_ruleset_version 42.0", "engine_ruleset_age_seconds 90.0",
                "engine_ruleset_load_failures_total{reason=\"compile\"} 2.0", "engine_ruleset_load_failures_total{reason=\"kafka\"} 1.0");
    }

    @Test
    @DisplayName("versão ignorada (reenvio) não mexe na versão em uso")
    void ignorada() {
        metrics.loaded(42, NOW);
        metrics.ignored(41);

        assertThat(scrape()).contains("engine_ruleset_version 42.0");
    }
}
