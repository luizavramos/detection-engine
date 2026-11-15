package br.com.motorfraude.engine.adapters.gateway.kafka;

import br.com.motorfraude.engine.domain.features.AccountFeatures;
import br.com.motorfraude.engine.domain.features.BucketRing;
import br.com.motorfraude.engine.domain.features.RecentSet;
import java.io.ByteArrayOutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public final class AccountFeaturesSerde {

    static final byte VERSION = 1;

    private AccountFeaturesSerde() {
    }

    public static Serde<AccountFeatures> kafkaSerde() {
        return new Serde<>() {
            @Override
            public Serializer<AccountFeatures> serializer() {
                return (topic, value) -> value == null ? null : serialize(value);
            }

            @Override
            public Deserializer<AccountFeatures> deserializer() {
                return (topic, bytes) -> bytes == null ? null : deserialize(bytes);
            }
        };
    }

    public static byte[] serialize(AccountFeatures features) {
        var out = new ByteArrayOutputStream(1024);
        out.write(VERSION);
        writeRing(out, features.minutes());
        writeRing(out, features.hours());
        writeRing(out, features.days());
        writeSet(out, features.beneficiaries());
        writeSet(out, features.devices());
        return out.toByteArray();
    }

    public static AccountFeatures deserialize(byte[] bytes) {
        if (bytes.length == 0) {
            throw new IllegalArgumentException("AccountFeatures vazio");
        }
        if (bytes[0] != VERSION) {
            throw new IllegalArgumentException("versão desconhecida do AccountFeatures: " + bytes[0]);
        }
        try {
            var in = ByteBuffer.wrap(bytes, 1, bytes.length - 1);
            var minutes = readRing(in, new BucketRing(AccountFeatures.MINUTES));
            var hours = readRing(in, new BucketRing(AccountFeatures.HOURS));
            var days = readRing(in, new BucketRing(AccountFeatures.DAYS));
            var beneficiaries = readSet(in, new RecentSet(AccountFeatures.BENEFICIARIES));
            var devices = readSet(in, new RecentSet(AccountFeatures.DEVICES));
            if (in.hasRemaining()) {
                throw new IllegalArgumentException("sobraram bytes no AccountFeatures");
            }
            return new AccountFeatures(minutes, hours, days, beneficiaries, devices);
        } catch (BufferUnderflowException | IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("AccountFeatures truncado", e);
        }
    }

    private static void writeRing(ByteArrayOutputStream out, BucketRing ring) {
        var slots = new ArrayList<Integer>();
        for (int i = 0; i < ring.size(); i++) {
            if (ring.epochAt(i) != BucketRing.EMPTY) {
                slots.add(i);
            }
        }
        slots.sort(Comparator.comparingLong(ring::epochAt));
        writeVarLong(out, slots.size());
        if (slots.isEmpty()) {
            return;
        }
        long base = ring.epochAt(slots.getLast());
        writeVarLong(out, zigzag(base));
        for (int slot : slots) {
            writeVarLong(out, base - ring.epochAt(slot));
            writeVarLong(out, zigzag(ring.countAt(slot)));
            writeVarLong(out, zigzag(ring.sumAt(slot)));
            writeVarLong(out, zigzag(ring.maxAt(slot)));
        }
    }

    private static BucketRing readRing(ByteBuffer in, BucketRing ring) {
        long n = readVarLong(in);
        if (n > ring.size()) {
            throw new IllegalArgumentException("slots demais no AccountFeatures: " + n);
        }
        if (n == 0) {
            return ring;
        }
        long base = unzigzag(readVarLong(in));
        for (long i = 0; i < n; i++) {
            long epoch = base - readVarLong(in);
            ring.load(epoch, unzigzag(readVarLong(in)), unzigzag(readVarLong(in)), unzigzag(readVarLong(in)));
        }
        return ring;
    }

    private static void writeSet(ByteArrayOutputStream out, RecentSet set) {
        List<String> tokens = set.tokens();
        writeVarLong(out, tokens.size());
        if (tokens.isEmpty()) {
            return;
        }
        long base = Long.MIN_VALUE;
        for (var token : tokens) {
            base = Math.max(base, set.lastSeen(token));
        }
        writeVarLong(out, zigzag(base));
        for (var token : tokens) {
            var utf8 = token.getBytes(StandardCharsets.UTF_8);
            writeVarLong(out, utf8.length);
            out.write(utf8, 0, utf8.length);
            writeVarLong(out, base - set.lastSeen(token));
        }
    }

    private static RecentSet readSet(ByteBuffer in, RecentSet set) {
        long n = readVarLong(in);
        if (n > set.limit()) {
            throw new IllegalArgumentException("itens demais no AccountFeatures: " + n);
        }
        if (n == 0) {
            return set;
        }
        long base = unzigzag(readVarLong(in));
        for (long i = 0; i < n; i++) {
            int length = (int) readVarLong(in);
            if (length < 0 || length > in.remaining()) {
                throw new IllegalArgumentException("AccountFeatures truncado");
            }
            var utf8 = new byte[length];
            in.get(utf8);
            set.touch(new String(utf8, StandardCharsets.UTF_8), base - readVarLong(in));
        }
        return set;
    }

    private static long zigzag(long value) {
        return (value << 1) ^ (value >> 63);
    }

    private static long unzigzag(long value) {
        return (value >>> 1) ^ -(value & 1);
    }

    private static void writeVarLong(ByteArrayOutputStream out, long value) {
        while ((value & ~0x7FL) != 0) {
            out.write((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((int) value);
    }

    private static long readVarLong(ByteBuffer in) {
        long result = 0;
        int shift = 0;
        while (true) {
            if (shift > 63) {
                throw new IllegalArgumentException("varint inválido no AccountFeatures");
            }
            byte b = in.get();
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
    }
}
