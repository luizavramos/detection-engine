package br.com.motorfraude.engine.infrastructure;

import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.commons.tracing.StandardHeaders;
import br.com.motorfraude.contracts.Channel;
import br.com.motorfraude.contracts.ContractAssertions;
import br.com.motorfraude.contracts.Counterparty;
import br.com.motorfraude.contracts.CustomerProfile;
import br.com.motorfraude.contracts.EvaluateTransaction;
import br.com.motorfraude.contracts.FraudDecision;
import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.contracts.Money;
import br.com.motorfraude.contracts.Segment;
import br.com.motorfraude.contracts.TransactionHistoryRecord;
import br.com.motorfraude.contracts.TransactionType;
import br.com.motorfraude.engine.InitialRules;
import br.com.motorfraude.engine.adapters.gateway.kafka.DlqDeserializationHandler;
import br.com.motorfraude.engine.fakes.FixedRuleSetProvider;
import br.com.motorfraude.engine.fakes.RecordingEngineMetrics;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spec 006, SC-001: depois do backfill, a {@code avgAmount30d} de 100 contas de referência no motor bate com o cálculo
 * direto em SQL no banco legado, diferença 0.
 *
 * <p>O motor não expõe features fora das regras, então a prova roda em processo: a topologia real recebe, no tópico
 * de backfill, os registros que o {@code transaction-service} publicou (fixture {@code reference-history.jsonl},
 * gerada pelo {@code TransactionServiceIT} contra Postgres, Kafka e Debezium reais) e avalia a primeira transação ao
 * vivo de cada conta, em {@code cutoverAt}; a feature sai na própria decisão.
 *
 * <p>A referência em SQL ({@code reference-expected.json}) usa a mesma janela que o motor: {@code avgAmount30d} é a
 * soma dos buckets dos 30 dias UTC até o dia do corte dividida pela contagem (divisão inteira), calculada antes da
 * transação avaliada; {@code maxAmount90d}, o maior valor dos 90 dias UTC.
 */
class BackfillReferenceAccuracyTest {

    private static final String RESOURCE_DIR = "/backfill-reference/";

    @TempDir
    Path stateDir;

    private static String resource(String name) {
        try (var in = BackfillReferenceAccuracyTest.class.getResourceAsStream(RESOURCE_DIR + name)) {
            assertThat(in).as("fixture %s não encontrada; gere com o TransactionServiceIT (-Dfixture.dir)", name).isNotNull();
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    @DisplayName("SC-001: avgAmount30d, maxAmount90d e count30d de 100 contas no motor batem com o SQL, diferença 0")
    void backfillBateComOSql() {
        var expected = JsonCodec.readTree(resource("reference-expected.json"));
        var cutover = Instant.parse(expected.path("cutoverAt").asString());
        var accounts = new ArrayList<String>();
        expected.path("accounts").propertyNames().forEach(accounts::add);
        assertThat(accounts).as("contas de referência").hasSize(100);

        var records = resource("reference-history.jsonl").lines().filter(l -> !l.isBlank()).toList();
        assertThat(records).as("registros do backfill").hasSizeGreaterThan(500);

        var clock = Clock.fixed(cutover.plusMillis(120), ZoneOffset.UTC);
        var dlq = new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        var metrics = new RecordingEngineMetrics();
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "detection-engine-sc001");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        props.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, DlqDeserializationHandler.class);
        props.put(DlqDeserializationHandler.PRODUCER_CONFIG, dlq);
        props.put(DlqDeserializationHandler.METRICS_CONFIG, metrics);

        try (var driver = new TopologyTestDriver(TopologyFactory.create(new FixedRuleSetProvider(InitialRules.compiled(7)), clock, metrics, true),
                props, cutover)) {
            var backfill = driver.createInputTopic(TopologyFactory.BACKFILL_TOPIC, new StringSerializer(), new StringSerializer());
            var profiles = driver.createInputTopic(TopologyFactory.PROFILES_TOPIC, new StringSerializer(), new StringSerializer());
            var commands = driver.createInputTopic(TopologyFactory.COMMANDS_TOPIC, new StringSerializer(), new StringSerializer());
            var decisions = driver.createOutputTopic(TopologyFactory.DECISIONS_TOPIC, new StringDeserializer(), new StringDeserializer());

            // 1. o backfill, exatamente como o transaction-service o publicou (a chave é o accountId)
            for (var line : records) {
                ContractAssertions.assertProduces("transaction-history-record.v1", line);
                var record = JsonCodec.fromJson(line, TransactionHistoryRecord.class);
                backfill.pipeInput(record.accountId(), line);
            }
            assertThat(decisions.isEmpty()).as("o backfill não decide nada").isTrue();

            // 2. a primeira transação ao vivo de cada conta, em cutoverAt
            int n = 0;
            for (var account : accounts) {
                var profile = new CustomerProfile(account, Segment.PF, cutover.minus(Duration.ofDays(400)), null, List.of(), cutover);
                profiles.pipeInput(account, JsonCodec.toJson(profile));
                var tx = new EvaluateTransaction(1, "live-%08d-sc001".formatted(++n), account, "cus_tok_" + account.substring(8),
                        TransactionType.PIX, new Money(5_000, "BRL"), new Counterparty("acc_tok_dest0001", "60746948"), Channel.MOBILE,
                        "dev_aaaaaaaa", cutover);
                var headers = new RecordHeaders();
                headers.add(StandardHeaders.RECEIVED_AT, cutover.toString().getBytes(StandardCharsets.UTF_8));
                headers.add(StandardHeaders.TRACEPARENT, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".getBytes(StandardCharsets.UTF_8));
                commands.pipeInput(new TestRecord<>(account, JsonCodec.toJson(tx), headers, cutover));
            }

            var features = new HashMap<String, Map<String, Object>>();
            for (var record : decisions.readRecordsToList()) {
                var decision = JsonCodec.fromJson(record.getValue(), FraudDecision.class);
                features.put(decision.accountId(), decision.features());
            }
            assertThat(features).as("uma decisão por conta").hasSize(100);

            // 3. diferença 0, conta a conta, contra o SQL
            long withHistory = 0;
            for (var account : accounts) {
                var sql = expected.path("accounts").path(account);
                var engine = features.get(account);
                assertThat(((Number) engine.get("features.avgAmount30d")).longValue())
                        .as("avgAmount30d de %s", account).isEqualTo(sql.path("avgAmount30d").asLong());
                assertThat(((Number) engine.get("features.maxAmount90d")).longValue())
                        .as("maxAmount90d de %s", account).isEqualTo(sql.path("maxAmount90d").asLong());
                // count30d já inclui a própria transação avaliada
                assertThat(((Number) engine.get("features.count30d")).longValue())
                        .as("count30d de %s", account).isEqualTo(sql.path("count30d").asLong() + 1);
                if (sql.path("count30d").asLong() > 0) {
                    withHistory++;
                }
            }
            assertThat(withHistory).as("a prova só vale se as contas tiverem histórico nos 30 dias").isGreaterThanOrEqualTo(90);
        }
    }
}
