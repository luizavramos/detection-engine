package br.com.motorfraude.engine.domain.features;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BucketRingTest {

    @Test
    @DisplayName("anel vazio responde zero em tudo")
    void vazio() {
        var ring = new BucketRing(60);

        assertThat(ring.count(1000, 5)).isZero();
        assertThat(ring.sum(1000, 5)).isZero();
        assertThat(ring.max(1000, 5)).isZero();
    }

    @Test
    @DisplayName("a janela de 5 unidades inclui a unidade atual e as 4 anteriores, e nada além")
    void janela() {
        var ring = new BucketRing(60);
        ring.add(100, 10);
        ring.add(96, 20);
        ring.add(95, 40);

        assertThat(ring.count(100, 5)).isEqualTo(2);
        assertThat(ring.sum(100, 5)).isEqualTo(30);
        assertThat(ring.max(100, 5)).isEqualTo(20);
        assertThat(ring.count(100, 6)).isEqualTo(3);
    }

    @Test
    @DisplayName("rotação: uma unidade nova no mesmo slot zera o conteúdo velho")
    void rotacao() {
        var ring = new BucketRing(10);
        ring.add(5, 100);
        ring.add(15, 7);

        assertThat(ring.count(15, 10)).isEqualTo(1);
        assertThat(ring.sum(15, 10)).isEqualTo(7);
    }

    @Test
    @DisplayName("evento velho demais (já sobrescrito) é recusado e não apaga o dado novo")
    void eventoVelhoDemais() {
        var ring = new BucketRing(10);
        ring.add(15, 7);

        assertThat(ring.add(5, 100)).isFalse();
        assertThat(ring.sum(15, 10)).isEqualTo(7);
    }

    @Test
    @DisplayName("evento fora de ordem dentro do anel entra no bucket do seu instante")
    void foraDeOrdem() {
        var ring = new BucketRing(60);
        ring.add(100, 1);

        assertThat(ring.add(98, 5)).isTrue();
        assertThat(ring.count(98, 1)).isEqualTo(1);
        assertThat(ring.sum(100, 3)).isEqualTo(6);
    }

    @Test
    @DisplayName("tamanho e janela inválidos são rejeitados")
    void invalidos() {
        assertThatThrownBy(() -> new BucketRing(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BucketRing(10).count(5, 11)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BucketRing(10).count(5, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("os slots podem ser lidos e recarregados (base do serde)")
    void slots() {
        var ring = new BucketRing(10);
        ring.add(42, 300);
        ring.add(42, 500);

        var copy = new BucketRing(10);
        for (int i = 0; i < ring.size(); i++) {
            if (ring.epochAt(i) != BucketRing.EMPTY) {
                copy.load(ring.epochAt(i), ring.countAt(i), ring.sumAt(i), ring.maxAt(i));
            }
        }

        assertThat(copy.count(42, 1)).isEqualTo(2);
        assertThat(copy.sum(42, 1)).isEqualTo(800);
        assertThat(copy.max(42, 1)).isEqualTo(500);
    }
}
