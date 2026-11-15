package br.com.motorfraude.engine.infrastructure;

import static br.com.motorfraude.engine.RuleSetFixtures.json;
import static br.com.motorfraude.engine.RuleSetFixtures.ruleSet;
import static br.com.motorfraude.engine.RuleSetFixtures.withParam;
import static br.com.motorfraude.engine.RuleSetFixtures.withRule;
import static br.com.motorfraude.engine.RuleSetFixtures.withStatus;
import static br.com.motorfraude.engine.RuleSetFixtures.without;
import static br.com.motorfraude.engine.TestData.tx;
import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.contracts.ContractAssertions;
import br.com.motorfraude.contracts.FraudDecision;
import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.contracts.Severity;
import br.com.motorfraude.engine.adapters.gateway.rules.KafkaRuleSetProvider;
import br.com.motorfraude.engine.adapters.gateway.rules.RuleSetLoader;
import br.com.motorfraude.engine.application.gateway.RuleSetMetrics;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HotRulesTest {

    private static final Instant THREE_AM_SP = Instant.parse("2026-11-10T06:00:30Z");
    private static final TopicPartition PARTITION = new TopicPartition(KafkaRuleSetProvider.TOPIC, 0);

    @TempDir
    Path stateDir;

    private final MockConsumer<String, String> consumer = new MockConsumer<>("earliest");
    private KafkaRuleSetProvider provider;
    private TopologyTestDriver driver;
    private TestInputTopic<String, String> commands;
    private TestOutputTopic<String, String> decisions;
    private long offset;
    private int sequence;

    @BeforeEach
    void setUp() {
        consumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
        consumer.updateEndOffsets(Map.of(PARTITION, 0L));
        provider = new KafkaRuleSetProvider(consumer, KafkaRuleSetProvider.TOPIC, new RuleSetLoader(RuleSetMetrics.NOOP), RuleSetMetrics.NOOP);
        provider.initialize();

        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "detection-engine-hot-rules");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        driver = new TopologyTestDriver(TopologyFactory.create(provider, Clock.fixed(THREE_AM_SP, ZoneOffset.UTC), true), props, THREE_AM_SP);
        commands = driver.createInputTopic(TopologyFactory.COMMANDS_TOPIC, new StringSerializer(), new StringSerializer());
        decisions = driver.createOutputTopic(TopologyFactory.DECISIONS_TOPIC, new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
        provider.close();
    }

    private void publish(tools.jackson.databind.node.ObjectNode set) {
        consumer.addRecord(new ConsumerRecord<>(KafkaRuleSetProvider.TOPIC, 0, offset++, "active", json(set)));
        consumer.updateEndOffsets(Map.of(PARTITION, offset));
        provider.pollOnce(Duration.ZERO);
    }

    private FraudDecision decide() {
        var id = "tx-hot-%04d-abcdef".formatted(++sequence);
        var transaction = tx(id, THREE_AM_SP.plusSeconds(sequence), 150_000, "acc_tok_dest0001", "dev_aaaaaaaa");
        commands.pipeInput(transaction.accountId(), JsonCodec.toJson(transaction));
        var record = decisions.readRecord();
        ContractAssertions.assertProduces("fraud-decision.v1", record.getValue());
        return JsonCodec.fromJson(record.getValue(), FraudDecision.class);
    }

    private static boolean hit(FraudDecision decision, String ruleId) {
        return decision.triggeredRules().stream().anyMatch(h -> h.ruleId().equals(ruleId));
    }

    @Test
    @DisplayName("H5: regra nova, mudança de threshold, desligamento e rollback valem nas decisões seguintes, na mesma topologia, sem reiniciar")
    void regrasAQuente() {
        publish(ruleSet(1));
        var base = decide();
        assertThat(base.ruleSetVersion()).isEqualTo(1);
        assertThat(hit(base, "R05")).isTrue();
        assertThat(base.severity()).isEqualTo(Severity.NONE);

        var comRegraNova = withRule(ruleSet(2), "R20", "tx.amount > 100000", 70, "ACTIVE");
        publish(comRegraNova);
        var nova = decide();
        assertThat(nova.ruleSetVersion()).isEqualTo(2);
        assertThat(hit(nova, "R20")).isTrue();
        assertThat(nova.severity()).isEqualTo(Severity.HIGH);

        publish(withParam(withRule(ruleSet(3), "R20", "tx.amount > 100000", 70, "ACTIVE"), "R05", "minAmount", 500_000));
        var threshold = decide();
        assertThat(threshold.ruleSetVersion()).isEqualTo(3);
        assertThat(hit(threshold, "R05")).isFalse();
        assertThat(hit(threshold, "R20")).isTrue();

        publish(without(ruleSet(4), "R20"));
        var desligada = decide();
        assertThat(desligada.ruleSetVersion()).isEqualTo(4);
        assertThat(hit(desligada, "R20")).isFalse();

        publish(withRule(ruleSet(5), "R20", "tx.amount > 100000", 70, "ACTIVE"));
        var rollback = decide();
        assertThat(rollback.ruleSetVersion()).isEqualTo(5);
        assertThat(hit(rollback, "R20")).isTrue();
        assertThat(rollback.severity()).isEqualTo(Severity.HIGH);
    }

    @Test
    @DisplayName("H5: regra levada para sombra deixa de pontuar, mas continua visível em shadowHits")
    void regraEmSombra() {
        publish(withRule(ruleSet(1), "R20", "tx.amount > 100000", 70, "ACTIVE"));
        assertThat(decide().severity()).isEqualTo(Severity.HIGH);

        publish(withStatus(withRule(ruleSet(2), "R20", "tx.amount > 100000", 70, "ACTIVE"), "R20", "SHADOW"));
        var shadow = decide();

        assertThat(shadow.ruleSetVersion()).isEqualTo(2);
        assertThat(hit(shadow, "R20")).isFalse();
        assertThat(shadow.shadowHits()).extracting("ruleId").contains("R20");
        assertThat(shadow.severity()).isEqualTo(Severity.NONE);
    }

    @Test
    @DisplayName("H5: um conjunto que não compila no meio do caminho não interrompe as decisões: segue a versão anterior")
    void conjuntoQuebradoNaoParaOMotor() {
        publish(ruleSet(1));
        assertThat(decide().ruleSetVersion()).isEqualTo(1);

        publish(withRule(ruleSet(2), "R99", "features.naoExiste > 1", 10, "ACTIVE"));
        var depois = decide();

        assertThat(depois.ruleSetVersion()).isEqualTo(1);
    }
}
