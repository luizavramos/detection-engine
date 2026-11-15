package br.com.motorfraude.engine.adapters.gateway.rules;

import static br.com.motorfraude.engine.RuleSetFixtures.json;
import static br.com.motorfraude.engine.RuleSetFixtures.ruleSet;
import static br.com.motorfraude.engine.RuleSetFixtures.withBrokenRule;
import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.engine.adapters.gateway.rules.RuleSetLoaderTest.RecordingMetrics;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers(disabledWithoutDocker = true)
class KafkaRuleSetProviderIT {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.0");

    private static KafkaConsumer<String, String> consumer() {
        return new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
    }

    private static void publish(String json) throws Exception {
        try (var producer = new KafkaProducer<String, String>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            producer.send(new ProducerRecord<>(KafkaRuleSetProvider.TOPIC, "active", json)).get();
        }
    }

    private static boolean await(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(100);
        }
        return condition.getAsBoolean();
    }

    @Test
    @DisplayName("Kafka real: tópico vazio sem prontidão; evento novo troca; versão repetida e conjunto quebrado são ignorados; instância nova lê o último")
    void cicloCompleto() throws Exception {
        try (var admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(KafkaRuleSetProvider.TOPIC, 1, (short) 1)
                    .configs(Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)))).all().get();
        }
        var metrics = new RecordingMetrics();
        var first = new KafkaRuleSetProvider(consumer(), KafkaRuleSetProvider.TOPIC, new RuleSetLoader(metrics), metrics);
        first.start();
        try {
            Thread.sleep(1_500);
            assertThat(first.ready()).as("tópico vazio").isFalse();

            publish(json(ruleSet(41)));
            assertThat(await(first::ready, Duration.ofSeconds(30))).isTrue();
            assertThat(first.current().orElseThrow().version()).isEqualTo(41);

            publish(json(ruleSet(42)));
            assertThat(await(() -> first.current().orElseThrow().version() == 42, Duration.ofSeconds(30))).isTrue();

            publish(json(ruleSet(42)));
            publish(json(withBrokenRule(ruleSet(43))));
            assertThat(await(() -> metrics.failures.contains("compile") && metrics.ignored >= 1, Duration.ofSeconds(30))).isTrue();
            assertThat(first.current().orElseThrow().version()).isEqualTo(42);
        } finally {
            first.close();
        }

        var metricsOfSecond = new RecordingMetrics();
        var second = new KafkaRuleSetProvider(consumer(), KafkaRuleSetProvider.TOPIC, new RuleSetLoader(metricsOfSecond), metricsOfSecond);
        second.start();
        try {
            assertThat(await(second::ready, Duration.ofSeconds(30))).isTrue();
            assertThat(second.current().orElseThrow().version()).isEqualTo(42);
        } finally {
            second.close();
        }
    }
}
