package br.com.motorfraude.engine.adapters.gateway.kafka;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DlqDeserializationHandler implements DeserializationExceptionHandler {

    public static final String PRODUCER_CONFIG = "engine.dlq.producer";
    public static final String TOPIC_CONFIG = "engine.dlq.topic";
    public static final String DEFAULT_TOPIC = "transaction-commands.dlq";
    public static final String REASON_HEADER = "dlq.reason";
    public static final String SOURCE_TOPIC_HEADER = "dlq.source.topic";
    public static final String SOURCE_PARTITION_HEADER = "dlq.source.partition";
    public static final String SOURCE_OFFSET_HEADER = "dlq.source.offset";

    private static final Logger LOG = LoggerFactory.getLogger(DlqDeserializationHandler.class);

    private Producer<byte[], byte[]> producer;
    private String topic = DEFAULT_TOPIC;

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs) {
        if (configs.get(TOPIC_CONFIG) != null) {
            topic = String.valueOf(configs.get(TOPIC_CONFIG));
        }
        if (configs.get(PRODUCER_CONFIG) instanceof Producer<?, ?> injected) {
            producer = (Producer<byte[], byte[]>) injected;
            return;
        }
        producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.valueOf(configs.get("bootstrap.servers")),
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));
    }

    @Override
    public DeserializationHandlerResponse handle(ErrorHandlerContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
        var dead = new ProducerRecord<>(topic, record.key(), record.value());
        for (Header header : record.headers()) {
            dead.headers().add(header);
        }
        dead.headers().add(REASON_HEADER, bytes(String.valueOf(exception.getMessage())));
        dead.headers().add(SOURCE_TOPIC_HEADER, bytes(record.topic()));
        dead.headers().add(SOURCE_PARTITION_HEADER, bytes(String.valueOf(record.partition())));
        dead.headers().add(SOURCE_OFFSET_HEADER, bytes(String.valueOf(record.offset())));
        try {
            producer.send(dead).get();
            LOG.warn("mensagem de {} [{}@{}] enviada para {}: {}", record.topic(), record.partition(), record.offset(), topic,
                    exception.getMessage());
            return DeserializationHandlerResponse.CONTINUE;
        } catch (Exception sendFailure) {
            if (sendFailure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            LOG.error("não foi possível enviar a mensagem de {} [{}@{}] para {}; interrompendo", record.topic(), record.partition(),
                    record.offset(), topic, sendFailure);
            return DeserializationHandlerResponse.FAIL;
        }
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
