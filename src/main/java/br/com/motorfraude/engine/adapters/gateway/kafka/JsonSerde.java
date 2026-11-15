package br.com.motorfraude.engine.adapters.gateway.kafka;

import br.com.motorfraude.contracts.JsonCodec;
import java.util.function.Consumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public final class JsonSerde<T> implements Serde<T> {

    private final Class<T> type;
    private final Consumer<T> validator;

    private JsonSerde(Class<T> type, Consumer<T> validator) {
        this.type = type;
        this.validator = validator;
    }

    public static <T> JsonSerde<T> of(Class<T> type) {
        return new JsonSerde<>(type, value -> {
        });
    }

    public static <T> JsonSerde<T> validated(Class<T> type, Consumer<T> validator) {
        return new JsonSerde<>(type, validator);
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, value) -> value == null ? null : JsonCodec.toBytes(value);
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null) {
                return null;
            }
            T value = JsonCodec.fromJson(bytes, type);
            validator.accept(value);
            return value;
        };
    }
}
