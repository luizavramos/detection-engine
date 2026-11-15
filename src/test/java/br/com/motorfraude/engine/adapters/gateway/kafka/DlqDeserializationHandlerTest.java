package br.com.motorfraude.engine.adapters.gateway.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler.DeserializationHandlerResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DlqDeserializationHandlerTest {

    private static ConsumerRecord<byte[], byte[]> record() {
        var headers = new RecordHeaders();
        headers.add("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".getBytes(StandardCharsets.UTF_8));
        var record = new ConsumerRecord<byte[], byte[]>("transaction-commands.v1", 3, 42L, 1000L,
                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, "acc_tok_00000001".getBytes(StandardCharsets.UTF_8),
                "{ruim".getBytes(StandardCharsets.UTF_8), headers, java.util.Optional.empty());
        return record;
    }

    @Test
    @DisplayName("envia o registro bruto para a DLQ com o motivo e a origem, e manda a partição continuar")
    void enviaEContinua() {
        var producer = new MockProducer<byte[], byte[]>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        var handler = new DlqDeserializationHandler();
        handler.configure(Map.of(DlqDeserializationHandler.PRODUCER_CONFIG, producer, DlqDeserializationHandler.TOPIC_CONFIG,
                "transaction-commands.dlq"));

        var response = handler.handle((org.apache.kafka.streams.errors.ErrorHandlerContext) null, record(), new IllegalArgumentException("JSON inválido"));

        assertThat(response).isEqualTo(DeserializationHandlerResponse.CONTINUE);
        assertThat(producer.history()).hasSize(1);
        var sent = producer.history().getFirst();
        assertThat(sent.topic()).isEqualTo("transaction-commands.dlq");
        assertThat(new String(sent.key(), StandardCharsets.UTF_8)).isEqualTo("acc_tok_00000001");
        assertThat(new String(sent.value(), StandardCharsets.UTF_8)).isEqualTo("{ruim");
        assertThat(header(sent, DlqDeserializationHandler.REASON_HEADER)).contains("JSON inválido");
        assertThat(header(sent, DlqDeserializationHandler.SOURCE_TOPIC_HEADER)).isEqualTo("transaction-commands.v1");
        assertThat(header(sent, DlqDeserializationHandler.SOURCE_PARTITION_HEADER)).isEqualTo("3");
        assertThat(header(sent, DlqDeserializationHandler.SOURCE_OFFSET_HEADER)).isEqualTo("42");
        assertThat(header(sent, "traceparent")).startsWith("00-4bf92f");
    }

    @Test
    @DisplayName("se a DLQ não aceitar o registro, falha em vez de descartar a mensagem em silêncio")
    void falhaSeADlqFalhar() {
        var producer = new MockProducer<byte[], byte[]>(true, null, new ByteArraySerializer(), new ByteArraySerializer()) {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
                throw new KafkaException("broker fora do ar");
            }
        };
        var handler = new DlqDeserializationHandler();
        handler.configure(Map.of(DlqDeserializationHandler.PRODUCER_CONFIG, producer));

        var response = handler.handle((org.apache.kafka.streams.errors.ErrorHandlerContext) null, record(), new IllegalArgumentException("x"));

        assertThat(response).isEqualTo(DeserializationHandlerResponse.FAIL);
    }

    @Test
    @DisplayName("registro sem chave nem corpo também vai para a DLQ")
    void semChaveNemCorpo() {
        var producer = new MockProducer<byte[], byte[]>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        var handler = new DlqDeserializationHandler();
        handler.configure(Map.of(DlqDeserializationHandler.PRODUCER_CONFIG, producer));
        var empty = new ConsumerRecord<byte[], byte[]>("transaction-commands.v1", 0, 1L, null, null);

        var response = handler.handle((org.apache.kafka.streams.errors.ErrorHandlerContext) null, empty, new IllegalArgumentException("vazio"));

        assertThat(response).isEqualTo(DeserializationHandlerResponse.CONTINUE);
        assertThat(producer.history()).hasSize(1);
        assertThat(producer.history().getFirst().topic()).isEqualTo(DlqDeserializationHandler.DEFAULT_TOPIC);
    }

    private static String header(ProducerRecord<byte[], byte[]> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
