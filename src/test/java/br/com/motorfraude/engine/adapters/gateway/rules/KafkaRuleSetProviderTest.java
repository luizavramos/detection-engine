package br.com.motorfraude.engine.adapters.gateway.rules;

import static br.com.motorfraude.engine.RuleSetFixtures.json;
import static br.com.motorfraude.engine.RuleSetFixtures.ruleSet;
import static br.com.motorfraude.engine.RuleSetFixtures.withBrokenRule;
import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.engine.adapters.gateway.rules.RuleSetLoaderTest.RecordingMetrics;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class KafkaRuleSetProviderTest {

    private static final String TOPIC = "rule-sets.v1";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

    private final MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
    private final RecordingMetrics metrics = new RecordingMetrics();
    private KafkaRuleSetProvider provider;
    private long offset;

    @BeforeEach
    void setUp() {
        consumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
        consumer.updateEndOffsets(Map.of(PARTITION, 0L));
        provider = new KafkaRuleSetProvider(consumer, TOPIC, new RuleSetLoader(metrics), metrics);
    }

    @AfterEach
    void tearDown() {
        provider.close();
    }

    private void publish(String value) {
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset++, "active", value));
        consumer.updateEndOffsets(Map.of(PARTITION, offset));
    }

    @Test
    @DisplayName("H5.4: tópico vazio, ambiente novo: sem conjunto e sem prontidão")
    void topicoVazio() {
        provider.initialize();
        provider.pollOnce(Duration.ZERO);

        assertThat(provider.current()).isEmpty();
        assertThat(provider.ready()).isFalse();
    }

    @Test
    @DisplayName("H5.1: o evento novo troca a versão em uso")
    void eventoNovoTroca() {
        provider.initialize();
        publish(json(ruleSet(41)));
        provider.pollOnce(Duration.ZERO);
        assertThat(provider.current().orElseThrow().version()).isEqualTo(41);
        assertThat(provider.ready()).isTrue();

        publish(json(ruleSet(42)));
        provider.pollOnce(Duration.ZERO);

        assertThat(provider.current().orElseThrow().version()).isEqualTo(42);
    }

    @Test
    @DisplayName("versão repetida ou menor é ignorada")
    void repetidaOuMenor() {
        provider.initialize();
        publish(json(ruleSet(42)));
        publish(json(ruleSet(42)));
        publish(json(ruleSet(40)));
        provider.pollOnce(Duration.ZERO);

        assertThat(provider.current().orElseThrow().version()).isEqualTo(42);
        assertThat(metrics.ignored).isEqualTo(2);
    }

    @Test
    @DisplayName("H5.2: conjunto que não compila mantém o anterior e incrementa a métrica de falha")
    void naoCompila() {
        provider.initialize();
        publish(json(ruleSet(41)));
        publish(json(withBrokenRule(ruleSet(42))));
        provider.pollOnce(Duration.ZERO);

        assertThat(provider.current().orElseThrow().version()).isEqualTo(41);
        assertThat(metrics.failures).containsExactly("compile");
    }

    @Test
    @DisplayName("Kafka fora do ar: mantém o conjunto atual, conta a falha e se recupera na próxima leitura")
    void kafkaForaDoAr() {
        provider.initialize();
        publish(json(ruleSet(41)));
        provider.pollOnce(Duration.ZERO);

        consumer.setPollException(new KafkaException("broker fora do ar"));
        provider.pollOnce(Duration.ZERO);

        assertThat(provider.current().orElseThrow().version()).isEqualTo(41);
        assertThat(metrics.failures).containsExactly("kafka");

        publish(json(ruleSet(42)));
        provider.pollOnce(Duration.ZERO);
        assertThat(provider.current().orElseThrow().version()).isEqualTo(42);
    }

    @Test
    @DisplayName("instância nova lê o histórico do tópico e termina no último evento")
    void instanciaNova() {
        provider.initialize();
        publish(json(ruleSet(40)));
        publish(json(ruleSet(41)));
        publish(json(ruleSet(42)));

        provider.pollOnce(Duration.ZERO);

        assertThat(provider.current().orElseThrow().version()).isEqualTo(42);
        assertThat(provider.ready()).isTrue();
    }

    @Test
    @DisplayName("a instância só fica pronta depois de alcançar o fim que o tópico tinha ao subir, para não decidir com versão velha")
    void prontoSoNoFimDoTopico() {
        consumer.assign(java.util.List.of(PARTITION));
        publish(json(ruleSet(40)));
        publish(json(ruleSet(41)));
        consumer.updateEndOffsets(Map.of(PARTITION, 3L));
        provider.initialize();

        provider.pollOnce(Duration.ZERO);
        assertThat(provider.current().orElseThrow().version()).isEqualTo(41);
        assertThat(provider.ready()).as("ainda falta o offset 2").isFalse();

        publish(json(ruleSet(42)));
        provider.pollOnce(Duration.ZERO);
        assertThat(provider.current().orElseThrow().version()).isEqualTo(42);
        assertThat(provider.ready()).isTrue();
    }

    @Test
    @DisplayName("tombstone no tópico é ignorado")
    void tombstone() {
        provider.initialize();
        publish(json(ruleSet(41)));
        publish(null);
        provider.pollOnce(Duration.ZERO);

        assertThat(provider.current().orElseThrow().version()).isEqualTo(41);
    }

    @Test
    @DisplayName("em thread própria: o conjunto chega sem ninguém chamar o provedor, e fechar encerra a leitura")
    void threadPropria() throws Exception {
        consumer.assign(java.util.List.of(PARTITION));
        publish(json(ruleSet(41)));
        provider.start();

        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!provider.ready() && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }

        assertThat(provider.ready()).isTrue();
        provider.close();
        assertThat(provider.running()).isFalse();
    }
}
