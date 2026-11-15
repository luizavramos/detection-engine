package br.com.motorfraude.engine.domain.features;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RecentSetTest {

    @Test
    @DisplayName("token nunca visto é novo; depois de registrado, deixa de ser")
    void novidade() {
        var set = new RecentSet(3);

        assertThat(set.contains("a")).isFalse();
        set.touch("a", 1000);
        assertThat(set.contains("a")).isTrue();
    }

    @Test
    @DisplayName("nunca passa do limite: o menos recentemente usado sai")
    void limite() {
        var set = new RecentSet(3);
        set.touch("a", 1);
        set.touch("b", 2);
        set.touch("c", 3);
        set.touch("d", 4);

        assertThat(set.size()).isEqualTo(3);
        assertThat(set.contains("a")).isFalse();
        assertThat(set.tokens()).containsExactly("b", "c", "d");
    }

    @Test
    @DisplayName("usar de novo um token o salva da remoção (LRU)")
    void lru() {
        var set = new RecentSet(3);
        set.touch("a", 1);
        set.touch("b", 2);
        set.touch("c", 3);
        set.touch("a", 4);
        set.touch("d", 5);

        assertThat(set.contains("a")).isTrue();
        assertThat(set.contains("b")).isFalse();
        assertThat(set.tokens()).containsExactly("c", "a", "d");
    }

    @Test
    @DisplayName("guarda o instante mais recente de cada token, mesmo se chegar um evento mais antigo depois")
    void ultimoVisto() {
        var set = new RecentSet(3);
        set.touch("a", 5000);
        set.touch("a", 3000);

        assertThat(set.lastSeen("a")).isEqualTo(5000);
        assertThat(set.lastSeen("zzz")).isEqualTo(RecentSet.NEVER);
    }

    @Test
    @DisplayName("conta quantos tokens foram vistos desde um instante (distintos nas últimas 24 h)")
    void distintosDesde() {
        var set = new RecentSet(10);
        set.touch("a", 1000);
        set.touch("b", 5000);
        set.touch("c", 9000);

        assertThat(set.countSince(5000)).isEqualTo(2);
        assertThat(set.countSince(0)).isEqualTo(3);
        assertThat(set.countSince(9001)).isZero();
    }

    @Test
    @DisplayName("limite inválido é rejeitado")
    void invalido() {
        assertThatThrownBy(() -> new RecentSet(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Provide
    Arbitrary<List<String>> touches() {
        return Arbitraries.of("a", "b", "c", "d", "e", "f", "g", "h", "i", "j").list().ofMaxSize(60);
    }

    @Property
    void nuncaExcedeOLimiteEGuardaOsMaisRecentes(@ForAll("touches") List<String> touches, @ForAll("limits") int limit) {
        var set = new RecentSet(limit);
        var model = new ArrayList<String>();
        long ts = 0;
        for (var token : touches) {
            set.touch(token, ++ts);
            model.remove(token);
            model.add(token);
            if (model.size() > limit) {
                model.removeFirst();
            }
        }

        assertThat(set.size()).isLessThanOrEqualTo(limit);
        assertThat(set.tokens()).containsExactlyElementsOf(model);
    }

    @Provide
    Arbitrary<Integer> limits() {
        return Arbitraries.integers().between(1, 12);
    }
}
