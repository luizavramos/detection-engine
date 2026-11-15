package br.com.motorfraude.engine.domain.features;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

class BucketRingProperties {

    record Event(long unit, long amount) {}

    private static final long BASE = 29_000_000L;

    @Provide
    Arbitrary<Integer> sizes() {
        return Arbitraries.integers().between(1, 100);
    }

    @Provide
    Arbitrary<List<Event>> events() {
        var event = Combinators.combine(Arbitraries.longs().between(BASE, BASE + 400), Arbitraries.longs().between(0, 5_000_000))
                .as(Event::new);
        return event.list().ofMinSize(0).ofMaxSize(200);
    }

    @Provide
    Arbitrary<List<Event>> eventsWithinTwoMinutes() {
        return Arbitraries.integers().between(1, 120).flatMap(count -> {
            var ordered = Arbitraries.longs().between(0, 2).list().ofSize(count);
            var amounts = Arbitraries.longs().between(1, 1_000_000).list().ofSize(count);
            return Combinators.combine(ordered, amounts).as((jitter, amount) -> {
                var list = new ArrayList<Event>();
                for (int i = 0; i < jitter.size(); i++) {
                    list.add(new Event(BASE + i / 3 - jitter.get(i), amount.get(i)));
                }
                return list;
            });
        });
    }

    private static List<Event> accepted(int size, List<Event> events) {
        var kept = new ArrayList<Event>();
        long maxSeen = Long.MIN_VALUE;
        for (var event : events) {
            if (maxSeen == Long.MIN_VALUE || event.unit() > maxSeen - size) {
                kept.add(event);
            }
            maxSeen = Math.max(maxSeen, event.unit());
        }
        return kept;
    }

    private static List<Event> inWindow(List<Event> kept, long ref, int window) {
        return kept.stream().filter(e -> e.unit() > ref - window && e.unit() <= ref).toList();
    }

    @Property
    void somaDaJanelaIgualAForcaBruta(@ForAll("sizes") int size, @ForAll("events") List<Event> events, @ForAll int windowSeed,
            @ForAll int refBack) {
        var ring = new BucketRing(size);
        events.forEach(e -> ring.add(e.unit(), e.amount()));
        if (events.isEmpty()) {
            return;
        }
        long maxSeen = events.stream().mapToLong(Event::unit).max().orElseThrow();
        int back = Math.floorMod(refBack, size);
        long ref = maxSeen - back;
        int window = 1 + Math.floorMod(windowSeed, size - back);
        var expected = inWindow(accepted(size, events), ref, window);

        assertThat(ring.count(ref, window)).isEqualTo(expected.size());
        assertThat(ring.sum(ref, window)).isEqualTo(expected.stream().mapToLong(Event::amount).sum());
        assertThat(ring.max(ref, window)).isEqualTo(expected.stream().mapToLong(Event::amount).max().orElse(0));
    }

    @Property
    void rotacaoNaoApagaDadoMaisNovoQuandoChegaEventoVelho(@ForAll("sizes") int size, @ForAll("events") List<Event> events) {
        var ring = new BucketRing(size);
        events.forEach(e -> ring.add(e.unit(), e.amount()));
        if (events.isEmpty()) {
            return;
        }
        long maxSeen = events.stream().mapToLong(Event::unit).max().orElseThrow();
        var expected = inWindow(accepted(size, events), maxSeen, size);

        assertThat(ring.count(maxSeen, size)).isEqualTo(expected.size());
    }

    @Property
    void foraDeOrdemAteDoisMinutosEntraNoBucketDoSeuInstante(@ForAll("eventsWithinTwoMinutes") List<Event> events) {
        var ring = new BucketRing(60);
        var perUnit = new java.util.TreeMap<Long, long[]>();
        for (var event : events) {
            assertThat(ring.add(event.unit(), event.amount())).isTrue();
            var cell = perUnit.computeIfAbsent(event.unit(), k -> new long[2]);
            cell[0]++;
            cell[1] += event.amount();
        }

        long ref = perUnit.lastKey();
        for (var entry : perUnit.entrySet()) {
            assertThat(ring.count(entry.getKey(), 1)).as("minuto %d", entry.getKey() - BASE).isEqualTo(entry.getValue()[0]);
            assertThat(ring.sum(entry.getKey(), 1)).isEqualTo(entry.getValue()[1]);
        }
        assertThat(ring.count(ref, 60)).isEqualTo(events.size());
    }

    @Property
    void somaDeJanelaMaiorNuncaEMenor(@ForAll("sizes") int size, @ForAll("events") List<Event> events, @ForAll int a, @ForAll int b) {
        var ring = new BucketRing(size);
        events.forEach(e -> ring.add(e.unit(), e.amount()));
        if (events.isEmpty()) {
            return;
        }
        long ref = events.stream().mapToLong(Event::unit).max().orElseThrow();
        int small = 1 + Math.floorMod(a, size);
        int large = Math.min(size, small + Math.floorMod(b, size));

        assertThat(ring.count(ref, large)).isGreaterThanOrEqualTo(ring.count(ref, small));
        assertThat(ring.sum(ref, large)).isGreaterThanOrEqualTo(ring.sum(ref, small));
        assertThat(ring.max(ref, large)).isGreaterThanOrEqualTo(ring.max(ref, small));
    }
}
