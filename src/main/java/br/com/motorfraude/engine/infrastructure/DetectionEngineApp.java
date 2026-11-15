package br.com.motorfraude.engine.infrastructure;

import br.com.motorfraude.engine.adapters.gateway.kafka.DlqDeserializationHandler;
import br.com.motorfraude.engine.application.gateway.RuleSetProvider;
import br.com.motorfraude.platform.streams.StreamsAppBootstrap;
import br.com.motorfraude.platform.streams.StreamsSettings;
import java.time.Clock;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DetectionEngineApp {

    public static final String APPLICATION_ID = "detection-engine";

    private static final Logger LOG = LoggerFactory.getLogger(DetectionEngineApp.class);

    private DetectionEngineApp() {
    }

    static Properties extraProperties() {
        var extra = new Properties();
        extra.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        extra.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, DlqDeserializationHandler.class);
        extra.put(DlqDeserializationHandler.TOPIC_CONFIG, TopologyFactory.DLQ_TOPIC);
        return extra;
    }

    public static StreamsAppBootstrap create(StreamsSettings settings, RuleSetProvider ruleSets, Clock clock) {
        return StreamsAppBootstrap.create(settings, extraProperties(), registry -> TopologyFactory.create(ruleSets, clock))
                .readinessCheck(() -> ruleSets.current().isPresent());
    }

    public static void main(String[] args) throws InterruptedException {
        var settings = StreamsSettings.fromEnvironment(APPLICATION_ID, System.getenv());
        RuleSetProvider ruleSets = Optional::empty;
        var app = create(settings, ruleSets, Clock.systemUTC());
        while (ruleSets.current().isEmpty()) {
            LOG.warn("aguardando o conjunto de regras (o KafkaRuleSetProvider entra na Fase 5 da spec 004); a topologia ainda não foi iniciada");
            Thread.sleep(5_000);
        }
        app.start();
    }
}
