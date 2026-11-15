package br.com.motorfraude.engine.infrastructure;

import br.com.motorfraude.engine.adapters.gateway.kafka.DlqDeserializationHandler;
import br.com.motorfraude.engine.adapters.gateway.micrometer.MicrometerEngineMetrics;
import br.com.motorfraude.engine.adapters.gateway.rules.KafkaRuleSetProvider;
import br.com.motorfraude.engine.adapters.gateway.rules.RuleSetLoader;
import br.com.motorfraude.engine.application.gateway.EngineMetrics;
import br.com.motorfraude.engine.application.gateway.RuleSetProvider;
import br.com.motorfraude.platform.streams.StreamsAppBootstrap;
import br.com.motorfraude.platform.streams.StreamsSettings;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.time.Clock;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DetectionEngineApp {

    public static final String APPLICATION_ID = "detection-engine";

    private static final Logger LOG = LoggerFactory.getLogger(DetectionEngineApp.class);

    private DetectionEngineApp() {
    }

    static Properties extraProperties(EngineMetrics metrics) {
        var extra = new Properties();
        extra.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        extra.put(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG, 60_000L);
        extra.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, DlqDeserializationHandler.class);
        extra.put(DlqDeserializationHandler.TOPIC_CONFIG, TopologyFactory.DLQ_TOPIC);
        extra.put(DlqDeserializationHandler.METRICS_CONFIG, metrics);
        return extra;
    }

    public static StreamsAppBootstrap create(StreamsSettings settings, RuleSetProvider ruleSets, Clock clock, PrometheusMeterRegistry registry,
            EngineMetrics metrics) {
        return StreamsAppBootstrap.create(settings, extraProperties(metrics), registry,
                meters -> TopologyFactory.create(ruleSets, clock, metrics))
                .readinessCheck(() -> ruleSets.current().isPresent());
    }

    public static void main(String[] args) throws InterruptedException {
        var settings = StreamsSettings.fromEnvironment(APPLICATION_ID, System.getenv());
        var clock = Clock.systemUTC();
        var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        var metrics = new MicrometerEngineMetrics(registry, clock);
        var ruleSets = new KafkaRuleSetProvider(ruleSetConsumer(settings), KafkaRuleSetProvider.TOPIC, new RuleSetLoader(metrics), metrics);
        ruleSets.start();
        var app = create(settings, ruleSets, clock, registry, metrics);
        while (!ruleSets.ready()) {
            LOG.warn("aguardando o primeiro conjunto de regras em {}; a topologia ainda não foi iniciada", KafkaRuleSetProvider.TOPIC);
            Thread.sleep(5_000);
        }
        app.start();
    }

    static KafkaConsumer<String, String> ruleSetConsumer(StreamsSettings settings) {
        return new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
    }
}
