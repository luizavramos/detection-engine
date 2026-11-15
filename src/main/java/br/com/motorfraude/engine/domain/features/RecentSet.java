package br.com.motorfraude.engine.domain.features;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class RecentSet {

    public static final long NEVER = Long.MIN_VALUE;

    private final int limit;
    private final LinkedHashMap<String, Long> lastSeen;

    public RecentSet(int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("o limite deve ser 1 ou mais");
        }
        this.limit = limit;
        this.lastSeen = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
                return size() > RecentSet.this.limit;
            }
        };
    }

    public boolean contains(String token) {
        return lastSeen.containsKey(token);
    }

    public void touch(String token, long epochMillis) {
        lastSeen.merge(token, epochMillis, Math::max);
    }

    public long lastSeen(String token) {
        return lastSeen.getOrDefault(token, NEVER);
    }

    public int countSince(long epochMillis) {
        int total = 0;
        for (long seen : lastSeen.values()) {
            if (seen >= epochMillis) {
                total++;
            }
        }
        return total;
    }

    public int size() {
        return lastSeen.size();
    }

    public int limit() {
        return limit;
    }

    public List<String> tokens() {
        return new ArrayList<>(lastSeen.keySet());
    }
}
