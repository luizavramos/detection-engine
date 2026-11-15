package br.com.motorfraude.engine.domain.features;

import java.util.Arrays;

public final class BucketRing {

    public static final long EMPTY = Long.MIN_VALUE;

    private final int size;
    private final long[] slotEpoch;
    private final long[] count;
    private final long[] sum;
    private final long[] max;

    public BucketRing(int size) {
        if (size < 1) {
            throw new IllegalArgumentException("o anel precisa de ao menos 1 slot");
        }
        this.size = size;
        this.slotEpoch = new long[size];
        this.count = new long[size];
        this.sum = new long[size];
        this.max = new long[size];
        Arrays.fill(slotEpoch, EMPTY);
    }

    public boolean add(long unit, long amount) {
        int idx = Math.floorMod(unit, size);
        if (slotEpoch[idx] != unit) {
            if (slotEpoch[idx] != EMPTY && slotEpoch[idx] > unit) {
                return false;
            }
            slotEpoch[idx] = unit;
            count[idx] = 0;
            sum[idx] = 0;
            max[idx] = 0;
        }
        count[idx]++;
        sum[idx] += amount;
        if (amount > max[idx]) {
            max[idx] = amount;
        }
        return true;
    }

    public long count(long refUnit, int window) {
        long total = 0;
        for (int i = 0; i < size; i++) {
            if (inWindow(i, refUnit, window)) {
                total += count[i];
            }
        }
        return total;
    }

    public long sum(long refUnit, int window) {
        long total = 0;
        for (int i = 0; i < size; i++) {
            if (inWindow(i, refUnit, window)) {
                total += sum[i];
            }
        }
        return total;
    }

    public long max(long refUnit, int window) {
        long best = 0;
        for (int i = 0; i < size; i++) {
            if (inWindow(i, refUnit, window) && max[i] > best) {
                best = max[i];
            }
        }
        return best;
    }

    public int size() {
        return size;
    }

    public long epochAt(int slot) {
        return slotEpoch[slot];
    }

    public long countAt(int slot) {
        return count[slot];
    }

    public long sumAt(int slot) {
        return sum[slot];
    }

    public long maxAt(int slot) {
        return max[slot];
    }

    public void load(long epoch, long slotCount, long slotSum, long slotMax) {
        int idx = Math.floorMod(epoch, size);
        slotEpoch[idx] = epoch;
        count[idx] = slotCount;
        sum[idx] = slotSum;
        max[idx] = slotMax;
    }

    private boolean inWindow(int slot, long refUnit, int window) {
        if (window < 1 || window > size) {
            throw new IllegalArgumentException("janela deve estar entre 1 e " + size);
        }
        long epoch = slotEpoch[slot];
        return epoch != EMPTY && epoch > refUnit - window && epoch <= refUnit;
    }
}
