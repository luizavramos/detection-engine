package br.com.motorfraude.engine.domain;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

public final class Uuids {

    public static final UUID NAMESPACE = UUID.fromString("6f1e4c2a-8d3b-4f5e-9a7c-2b1d0e3f4a5c");

    private Uuids() {
    }

    public static UUID v5(UUID namespace, String name) {
        try {
            var sha1 = MessageDigest.getInstance("SHA-1");
            sha1.update(ByteBuffer.allocate(16).putLong(namespace.getMostSignificantBits()).putLong(namespace.getLeastSignificantBits()).array());
            sha1.update(name.getBytes(StandardCharsets.UTF_8));
            var hash = sha1.digest();
            hash[6] = (byte) ((hash[6] & 0x0F) | 0x50);
            hash[8] = (byte) ((hash[8] & 0x3F) | 0x80);
            var buffer = ByteBuffer.wrap(hash, 0, 16);
            return new UUID(buffer.getLong(), buffer.getLong());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 indisponível", e);
        }
    }
}
