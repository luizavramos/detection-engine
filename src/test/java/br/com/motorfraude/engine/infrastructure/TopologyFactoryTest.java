package br.com.motorfraude.engine.infrastructure;

import static br.com.motorfraude.engine.TestData.ACCOUNT;
import static br.com.motorfraude.engine.TestData.profile;
import static br.com.motorfraude.engine.TestData.tx;
import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.commons.tracing.StandardHeaders;
import br.com.motorfraude.contracts.BlocklistEntry;
import br.com.motorfraude.contracts.ContextCompleteness;
import br.com.motorfraude.contracts.ContractAssertions;
import br.com.motorfraude.contracts.EntityType;
import br.com.motorfraude.contracts.EvaluateTransaction;
import br.com.motorfraude.contracts.FraudAlert;
import br.com.motorfraude.contracts.FraudDecision;
import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.contracts.Money;
import br.com.motorfraude.contracts.Segment;
import br.com.motorfraude.contracts.Severity;
import br.com.motorfraude.contracts.TransactionHistoryRecord;
import br.com.motorfraude.engine.InitialRules;
import br.com.motorfraude.engine.adapters.gateway.kafka.DlqDeserializationHandler;
import br.com.motorfraude.engine.fakes.FixedRuleSetProvider;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TopologyFactoryTest {

    private static final Instant NOW = Instant.parse("2026-11-10T15:00:30Z");
    private static final Instant RECEIVED = NOW.plusMillis(40);
    private static final Instant DECIDED = NOW.plusMillis(120);
    private static final String TRACE = "4bf92f3577b34da6a3ce929d0e0e4736";

    @TempDir
    Path stateDir;

    private final MockProducer<byte[], byte[]> dlqProducer = new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
    private final FixedRuleSetProvider ruleSets = new FixedRuleSetProvider(InitialRules.compiled(7));
    private TopologyTestDriver driver;
    private TestInputTopic<String, String> commands;
    private TestInputTopic<String, String> backfill;
    private TestInputTopic<String, String> profiles;
    private TestInputTopic<String, String> blocklist;
    private TestOutputTopic<String, String> decisions;
    private TestOutputTopic<String, String> alerts;

    @BeforeEach
    void setUp() {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "detection-engine-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        props.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, DlqDeserializationHandler.class);
        props.put(DlqDeserializationHandler.PRODUCER_CONFIG, dlqProducer);
        driver = new TopologyTestDriver(TopologyFactory.create(ruleSets, Clock.fixed(DECIDED, ZoneOffset.UTC), true), props, DECIDED);
        commands = driver.createInputTopic(TopologyFactory.COMMANDS_TOPIC, new StringSerializer(), new StringSerializer());
        backfill = driver.createInputTopic(TopologyFactory.BACKFILL_TOPIC, new StringSerializer(), new StringSerializer());
        profiles = driver.createInputTopic(TopologyFactory.PROFILES_TOPIC, new StringSerializer(), new StringSerializer());
        blocklist = driver.createInputTopic(TopologyFactory.BLOCKLIST_TOPIC, new StringSerializer(), new StringSerializer());
        decisions = driver.createOutputTopic(TopologyFactory.DECISIONS_TOPIC, new StringDeserializer(), new StringDeserializer());
        alerts = driver.createOutputTopic(TopologyFactory.ALERTS_TOPIC, new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private void sendCommand(EvaluateTransaction transaction) {
        var headers = new RecordHeaders();
        headers.add(StandardHeaders.RECEIVED_AT, RECEIVED.toString().getBytes(StandardCharsets.UTF_8));
        headers.add(StandardHeaders.TRACEPARENT, ("00-" + TRACE + "-00f067aa0ba902b7-01").getBytes(StandardCharsets.UTF_8));
        commands.pipeInput(new TestRecord<>(transaction.accountId(), JsonCodec.toJson(transaction), headers, NOW));
    }

    private void sendProfile(Instant opened, Instant credentialChange) {
        profiles.pipeInput(ACCOUNT, JsonCodec.toJson(profile(Segment.PF, opened, credentialChange)));
    }

    private void flag(String token, EntityType type) {
        blocklist.pipeInput(token, JsonCodec.toJson(new BlocklistEntry(token, type, "fraude confirmada", NOW)));
    }

    private List<FraudDecision> drainDecisions() {
        var all = new ArrayList<FraudDecision>();
        for (var record : decisions.readRecordsToList()) {
            ContractAssertions.assertProduces("fraud-decision.v1", record.getValue());
            assertThat(record.getKey()).isEqualTo(JsonCodec.fromJson(record.getValue(), FraudDecision.class).accountId());
            all.add(JsonCodec.fromJson(record.getValue(), FraudDecision.class));
        }
        return all;
    }

    private List<FraudAlert> drainAlerts() {
        var all = new ArrayList<FraudAlert>();
        for (var record : alerts.readRecordsToList()) {
            ContractAssertions.assertProduces("fraud-alert.v1", record.getValue());
            all.add(JsonCodec.fromJson(record.getValue(), FraudAlert.class));
        }
        return all;
    }

    private static EvaluateTransaction benign(String id) {
        return tx(id, NOW, 5_000, "acc_tok_dest0001", "dev_aaaaaaaa");
    }

    @Test
    @DisplayName("H1: toda transação gera uma decisão em fraud-decisions.v1, com chave = conta e receivedAt do cabeçalho")
    void decisaoPublicada() {
        sendProfile(NOW.minus(Duration.ofDays(400)), null);

        sendCommand(benign("tx-0001-abcdef"));

        var all = drainDecisions();
        assertThat(all).hasSize(1);
        assertThat(all.getFirst().transactionId()).isEqualTo("tx-0001-abcdef");
        assertThat(all.getFirst().severity()).isEqualTo(Severity.NONE);
        assertThat(all.getFirst().eventReceivedAt()).isEqualTo(RECEIVED);
        assertThat(all.getFirst().decidedAt()).isEqualTo(DECIDED);
        assertThat(drainAlerts()).isEmpty();
    }

    @Test
    @DisplayName("H1: só MEDIUM e HIGH viram alerta em fraud-alerts.v1, com o traceId do cabeçalho traceparent")
    void alertaSoDeMediumEmDiante() {
        sendProfile(NOW.minus(Duration.ofDays(400)), null);
        flag("acc_tok_dest0666", EntityType.ACCOUNT);

        sendCommand(benign("tx-0002-abcdef"));
        sendCommand(tx("tx-0003-abcdef", NOW, 5_000, "acc_tok_dest0666", "dev_aaaaaaaa"));

        assertThat(drainDecisions()).extracting(FraudDecision::severity).containsExactly(Severity.NONE, Severity.HIGH);
        var alertList = drainAlerts();
        assertThat(alertList).hasSize(1);
        assertThat(alertList.getFirst().transactionId()).isEqualTo("tx-0003-abcdef");
        assertThat(alertList.getFirst().severity()).isEqualTo(Severity.HIGH);
        assertThat(alertList.getFirst().traceId()).isEqualTo(TRACE);
        assertThat(alertList.getFirst().eventReceivedAt()).isEqualTo(RECEIVED);
    }

    @Test
    @DisplayName("H2: a mesma transação duas vezes gera uma decisão só")
    void dedup() {
        sendProfile(NOW.minus(Duration.ofDays(400)), null);

        sendCommand(benign("tx-0004-abcdef"));
        sendCommand(benign("tx-0004-abcdef"));

        assertThat(drainDecisions()).hasSize(1);
    }

    @Test
    @DisplayName("H4: o perfil vem da tabela local; sem perfil a decisão é PARTIAL e com perfil volta a ser FULL")
    void perfilDaTabelaLocal() {
        sendCommand(benign("tx-0005-abcdef"));
        sendProfile(NOW.minus(Duration.ofDays(400)), NOW.minus(Duration.ofHours(2)));
        sendCommand(tx("tx-0006-abcdef", NOW.plusSeconds(1), 250_000, "acc_tok_dest0777", "dev_novo0001"));

        var all = drainDecisions();
        assertThat(all.get(0).contextCompleteness()).isEqualTo(ContextCompleteness.PARTIAL);
        assertThat(all.get(1).contextCompleteness()).isEqualTo(ContextCompleteness.FULL);
        assertThat(all.get(1).features()).containsEntry("profile.segment", "PF");
        assertThat(all.get(1).triggeredRules()).extracting("ruleId").contains("R01", "R12");
    }

    @Test
    @DisplayName("H7: a lista de bloqueio global vale na hora e o tombstone a desfaz")
    void blocklistComTombstone() {
        sendProfile(NOW.minus(Duration.ofDays(400)), null);
        flag("acc_tok_dest0666", EntityType.ACCOUNT);
        sendCommand(tx("tx-0007-abcdef", NOW, 5_000, "acc_tok_dest0666", "dev_aaaaaaaa"));

        blocklist.pipeInput("acc_tok_dest0666", (String) null);
        sendCommand(tx("tx-0008-abcdef", NOW.plusSeconds(1), 5_000, "acc_tok_dest0666", "dev_aaaaaaaa"));

        var all = drainDecisions();
        assertThat(all.get(0).features()).containsEntry("lists.counterpartyFlagged", true);
        assertThat(all.get(0).severity()).isEqualTo(Severity.HIGH);
        assertThat(all.get(1).features()).containsEntry("lists.counterpartyFlagged", false);
        assertThat(all.get(1).severity()).isEqualTo(Severity.NONE);
    }

    @Test
    @DisplayName("H7: dispositivo marcado na lista acende lists.deviceFlagged")
    void blocklistDeDispositivo() {
        sendProfile(NOW.minus(Duration.ofDays(400)), null);
        flag("dev_aaaaaaaa", EntityType.DEVICE);

        sendCommand(benign("tx-0009-abcdef"));

        assertThat(drainDecisions().getFirst().features()).containsEntry("lists.deviceFlagged", true);
    }

    private void sendBackfill(String id, int daysBack, long amount) {
        var record = new TransactionHistoryRecord(id, ACCOUNT, new Money(amount, "BRL"), "acc_tok_dest0001", "dev_aaaaaaaa",
                NOW.minus(Duration.ofDays(daysBack)));
        backfill.pipeInput(ACCOUNT, JsonCodec.toJson(record));
    }

    @Test
    @DisplayName("H8: o backfill popula os buckets sem avaliar regra nem publicar nada")
    void backfillSemSaida() {
        sendProfile(NOW.minus(Duration.ofDays(400)), null);
        for (int day = 1; day <= 10; day++) {
            sendBackfill("hist-%02d-abcdef".formatted(day), day, 20_000);
        }
        assertThat(decisions.isEmpty()).isTrue();
        assertThat(alerts.isEmpty()).isTrue();

        sendCommand(benign("tx-0010-abcdef"));

        var features = drainDecisions().getFirst().features();
        assertThat(features).containsEntry("features.count30d", 11).containsEntry("features.avgAmount30d", 20_000)
                .containsEntry("features.isNewBeneficiary", false);
    }

    @Test
    @DisplayName("H8: registro de backfill repetido, ou de transação já avaliada, não conta duas vezes")
    void backfillRepetido() {
        sendProfile(NOW.minus(Duration.ofDays(400)), null);
        sendBackfill("hist-01-abcdef", 1, 20_000);
        sendBackfill("hist-01-abcdef", 1, 20_000);
        sendCommand(benign("tx-0011-abcdef"));
        backfill.pipeInput(ACCOUNT, JsonCodec.toJson(new TransactionHistoryRecord("tx-0011-abcdef", ACCOUNT, new Money(5_000, "BRL"),
                "acc_tok_dest0001", "dev_aaaaaaaa", NOW)));
        sendCommand(tx("tx-0012-abcdef", NOW.plusSeconds(1), 5_000, "acc_tok_dest0001", "dev_aaaaaaaa"));

        var all = drainDecisions();
        assertThat(all.get(0).features()).containsEntry("features.count30d", 2);
        assertThat(all.get(1).features()).containsEntry("features.count30d", 3);
    }

    @Test
    @DisplayName("H9: mensagem que não desserializa vai para a DLQ com o motivo, e a partição continua")
    void dlq() {
        sendProfile(NOW.minus(Duration.ofDays(400)), null);

        commands.pipeInput(ACCOUNT, "{não é json");
        commands.pipeInput(ACCOUNT, "{\"schemaVersion\":1,\"transactionId\":\"tx-sem-campos\"}");
        sendCommand(benign("tx-0013-abcdef"));

        assertThat(dlqProducer.history()).hasSize(2);
        var first = dlqProducer.history().getFirst();
        assertThat(first.topic()).isEqualTo(TopologyFactory.DLQ_TOPIC);
        assertThat(new String(first.value(), StandardCharsets.UTF_8)).isEqualTo("{não é json");
        assertThat(new String(first.headers().lastHeader(DlqDeserializationHandler.REASON_HEADER).value(), StandardCharsets.UTF_8)).isNotBlank();
        assertThat(new String(first.headers().lastHeader(DlqDeserializationHandler.SOURCE_TOPIC_HEADER).value(), StandardCharsets.UTF_8))
                .isEqualTo(TopologyFactory.COMMANDS_TOPIC);
        assertThat(drainDecisions()).extracting(FraudDecision::transactionId).containsExactly("tx-0013-abcdef");
    }

    @Test
    @DisplayName("sem o cabeçalho receivedAt, o instante de recebimento é o do registro")
    void semCabecalho() {
        sendProfile(NOW.minus(Duration.ofDays(400)), null);

        commands.pipeInput(new TestRecord<>(ACCOUNT, JsonCodec.toJson(benign("tx-0014-abcdef")), new RecordHeaders(), NOW.plusSeconds(3)));

        var decision = drainDecisions().getFirst();
        assertThat(decision.eventReceivedAt()).isEqualTo(NOW.plusSeconds(3));
    }

    @Test
    @DisplayName("sem conjunto de regras a topologia não decide: o erro sobe e nada é publicado")
    void semConjuntoDeRegras() {
        ruleSets.set(null);

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> sendCommand(benign("tx-0015-abcdef"))).isInstanceOf(RuntimeException.class);
        assertThat(decisions.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("limpeza do seen-tx: dentro de 24 h a duplicata continua barrada; passado o prazo o registro é descartado")
    void limpezaDoSeenTx() {
        sendProfile(NOW.minus(Duration.ofDays(400)), null);
        sendCommand(benign("tx-0016-abcdef"));
        assertThat(drainDecisions()).hasSize(1);

        driver.advanceWallClockTime(Duration.ofHours(23));
        sendCommand(benign("tx-0016-abcdef"));
        assertThat(drainDecisions()).isEmpty();

        driver.advanceWallClockTime(Duration.ofHours(2));
        sendCommand(benign("tx-0016-abcdef"));
        assertThat(drainDecisions()).hasSize(1);
    }
}
