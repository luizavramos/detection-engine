package br.com.motorfraude.engine.adapters.gateway.micrometer;

import br.com.motorfraude.contracts.ContextCompleteness;
import br.com.motorfraude.engine.application.gateway.EngineMetrics;
import br.com.motorfraude.engine.application.gateway.RuleSetMetrics;
import br.com.motorfraude.engine.domain.Decision;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class MicrometerEngineMetrics implements EngineMetrics, RuleSetMetrics {

    private static final Duration[] SLO_BUCKETS = {
            Duration.ofMillis(5), Duration.ofMillis(10), Duration.ofMillis(25), Duration.ofMillis(50), Duration.ofMillis(100),
            Duration.ofMillis(250), Duration.ofMillis(500), Duration.ofSeconds(1), Duration.ofMillis(2500), Duration.ofSeconds(5)};

    private final MeterRegistry registry;
    private final Timer endToEnd;
    private final Counter duplicates;
    private final Counter partial;
    private final Counter late;
    private final Counter dlq;
    private final AtomicLong version = new AtomicLong();
    private final AtomicReference<Instant> publishedAt = new AtomicReference<>();

    public MicrometerEngineMetrics(MeterRegistry registry, Clock clock) {
        this.registry = registry;
        this.endToEnd = Timer.builder("engine.e2e.latency")
                .description("Da recepção da transação (receivedAt) até a decisão publicada")
                .serviceLevelObjectives(SLO_BUCKETS)
                .register(registry);
        for (var severity : new String[] {"NONE", "MEDIUM", "HIGH"}) {
            registry.counter("engine.decisions", "severity", severity);
        }
        for (var reason : new String[] {"invalid", "compile", "kafka"}) {
            registry.counter("engine.ruleset.load.failures", "reason", reason);
        }
        this.duplicates = registry.counter("engine.duplicates.dropped");
        this.partial = registry.counter("engine.context.partial");
        this.late = registry.counter("engine.late.events");
        this.dlq = registry.counter("engine.dlq");
        Gauge.builder("engine.ruleset.version", version, AtomicLong::get).description("Versão do conjunto de regras em uso").register(registry);
        Gauge.builder("engine.ruleset.age", () -> {
            var published = publishedAt.get();
            return published == null ? 0.0 : Math.max(0, Duration.between(published, clock.instant()).toMillis()) / 1000.0;
        }).baseUnit("seconds").description("Idade do conjunto de regras em uso, desde a publicação").register(registry);
    }

    @Override
    public void decided(Decision decision, Duration latency) {
        var message = decision.decisionMessage();
        registry.counter("engine.decisions", "severity", message.severity().name()).increment();
        message.triggeredRules().forEach(hit -> registry.counter("engine.rule.hits", "rule_id", hit.ruleId(), "mode", "active").increment());
        message.shadowHits().forEach(hit -> registry.counter("engine.rule.hits", "rule_id", hit.ruleId(), "mode", "shadow").increment());
        decision.ruleErrors().forEach(error -> registry.counter("engine.rule.errors", "rule_id", error.ruleId()).increment());
        if (message.contextCompleteness() == ContextCompleteness.PARTIAL) {
            partial.increment();
        }
        endToEnd.record(latency.isNegative() ? Duration.ZERO : latency);
    }

    @Override
    public void duplicateDropped() {
        duplicates.increment();
    }

    @Override
    public void lateEvent() {
        late.increment();
    }

    @Override
    public void dlq() {
        dlq.increment();
    }

    @Override
    public void loaded(long loadedVersion, Instant published) {
        version.set(loadedVersion);
        publishedAt.set(published);
    }

    @Override
    public void ignored(long ignoredVersion) {
    }

    @Override
    public void failed(String reason) {
        registry.counter("engine.ruleset.load.failures", "reason", reason).increment();
    }
}
