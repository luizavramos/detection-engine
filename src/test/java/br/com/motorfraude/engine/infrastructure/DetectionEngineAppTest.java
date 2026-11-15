package br.com.motorfraude.engine.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.engine.InitialRules;
import br.com.motorfraude.engine.adapters.gateway.kafka.DlqDeserializationHandler;
import br.com.motorfraude.engine.fakes.FixedRuleSetProvider;
import br.com.motorfraude.platform.streams.StreamsSettings;
import java.nio.file.Path;
import java.time.Clock;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DetectionEngineAppTest {

    @Test
    @DisplayName("o motor usa commit de 100 ms e o handler de DLQ da topologia")
    void propriedades() {
        var props = DetectionEngineApp.extraProperties(br.com.motorfraude.engine.application.gateway.EngineMetrics.NOOP);

        assertThat(props.get(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG)).isEqualTo(100);
        assertThat(props.get(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG)).isEqualTo(DlqDeserializationHandler.class);
        assertThat(props.get(DlqDeserializationHandler.TOPIC_CONFIG)).isEqualTo(TopologyFactory.DLQ_TOPIC);
    }

    @Test
    @DisplayName("a prontidão exige o conjunto de regras carregado, além do Kafka Streams em RUNNING")
    void prontidao(@TempDir Path tmp) {
        var ruleSets = new FixedRuleSetProvider(null);
        var settings = new StreamsSettings("detection-engine-app-test", "localhost:9092", tmp, 1, 0, "detection-engine-app-test-0", 0);

        var registry = new io.micrometer.prometheusmetrics.PrometheusMeterRegistry(io.micrometer.prometheusmetrics.PrometheusConfig.DEFAULT);
        try (var app = DetectionEngineApp.create(settings, ruleSets, Clock.systemUTC(), registry, new br.com.motorfraude.engine.adapters.gateway.micrometer.MicrometerEngineMetrics(registry, Clock.systemUTC()))) {
            assertThat(app.ready()).isFalse();
            ruleSets.set(InitialRules.compiled(1));
            assertThat(app.ready()).isFalse();
        }
    }
}
