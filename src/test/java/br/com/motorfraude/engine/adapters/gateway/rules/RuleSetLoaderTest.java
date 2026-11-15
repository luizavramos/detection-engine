package br.com.motorfraude.engine.adapters.gateway.rules;

import static br.com.motorfraude.engine.RuleSetFixtures.json;
import static br.com.motorfraude.engine.RuleSetFixtures.ruleSet;
import static br.com.motorfraude.engine.RuleSetFixtures.withBrokenRule;
import static br.com.motorfraude.engine.RuleSetFixtures.withStatus;
import static org.assertj.core.api.Assertions.assertThat;

import br.com.motorfraude.engine.application.gateway.RuleSetMetrics;
import br.com.motorfraude.engine.domain.features.FeatureSnapshot;
import br.com.motorfraude.engine.TestData;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RuleSetLoaderTest {

    static final class RecordingMetrics implements RuleSetMetrics {
        final List<Long> loaded = new ArrayList<>();
        final List<Instant> publishedAt = new ArrayList<>();
        int ignored;
        final List<String> failures = new ArrayList<>();

        @Override
        public void loaded(long version, Instant published) {
            loaded.add(version);
            publishedAt.add(published);
        }

        @Override
        public void ignored(long version) {
            ignored++;
        }

        @Override
        public void failed(String reason) {
            failures.add(reason);
        }
    }

    private final RecordingMetrics metrics = new RecordingMetrics();
    private final RuleSetLoader loader = new RuleSetLoader(metrics);

    @Test
    @DisplayName("sem nenhum evento, não há conjunto de regras")
    void vazio() {
        assertThat(loader.current()).isEmpty();
        assertThat(loader.version()).isZero();
    }

    @Test
    @DisplayName("o primeiro conjunto válido é carregado, com versão e instante de publicação nas métricas")
    void primeiroConjunto() {
        var result = loader.apply(json(ruleSet(41)));

        assertThat(result).isEqualTo(RuleSetLoader.Result.LOADED);
        assertThat(loader.version()).isEqualTo(41);
        assertThat(loader.current().orElseThrow().version()).isEqualTo(41);
        assertThat(metrics.loaded).containsExactly(41L);
        assertThat(metrics.publishedAt).containsExactly(Instant.parse("2026-11-15T12:00:00Z"));
    }

    @Test
    @DisplayName("H5.1: um conjunto de versão maior troca o atual")
    void versaoMaiorTroca() {
        loader.apply(json(ruleSet(41)));

        var result = loader.apply(json(ruleSet(42)));

        assertThat(result).isEqualTo(RuleSetLoader.Result.LOADED);
        assertThat(loader.current().orElseThrow().version()).isEqualTo(42);
        assertThat(metrics.loaded).containsExactly(41L, 42L);
    }

    @Test
    @DisplayName("versão repetida ou menor é um reenvio: ignora, sem trocar nem contar falha")
    void versaoRepetidaOuMenor() {
        loader.apply(json(ruleSet(42)));

        assertThat(loader.apply(json(ruleSet(42)))).isEqualTo(RuleSetLoader.Result.IGNORED);
        assertThat(loader.apply(json(ruleSet(41)))).isEqualTo(RuleSetLoader.Result.IGNORED);

        assertThat(loader.version()).isEqualTo(42);
        assertThat(metrics.ignored).isEqualTo(2);
        assertThat(metrics.failures).isEmpty();
    }

    @Test
    @DisplayName("H5.2: conjunto que não compila mantém o anterior e conta falha de compilação")
    void naoCompila() {
        loader.apply(json(ruleSet(41)));

        var result = loader.apply(json(withBrokenRule(ruleSet(42))));

        assertThat(result).isEqualTo(RuleSetLoader.Result.REJECTED);
        assertThat(loader.version()).isEqualTo(41);
        assertThat(metrics.failures).containsExactly("compile");
    }

    @Test
    @DisplayName("JSON inválido ou fora do schema mantém o anterior e conta falha de formato")
    void formatoInvalido() {
        loader.apply(json(ruleSet(41)));
        var semRegras = ruleSet(43);
        semRegras.remove("rules");

        assertThat(loader.apply("{não é json")).isEqualTo(RuleSetLoader.Result.REJECTED);
        assertThat(loader.apply(json(semRegras))).isEqualTo(RuleSetLoader.Result.REJECTED);
        assertThat(loader.apply("")).isEqualTo(RuleSetLoader.Result.REJECTED);

        assertThat(loader.version()).isEqualTo(41);
        assertThat(metrics.failures).containsExactly("invalid", "invalid", "invalid");
    }

    @Test
    @DisplayName("mensagem nula (tombstone) é ignorada sem alterar nada")
    void tombstone() {
        loader.apply(json(ruleSet(41)));

        assertThat(loader.apply(null)).isEqualTo(RuleSetLoader.Result.IGNORED);
        assertThat(loader.version()).isEqualTo(41);
    }

    @Test
    @DisplayName("regras em SHADOW chegam como sombra: o conjunto compilado separa os acertos")
    void sombra() {
        loader.apply(json(withStatus(ruleSet(44), "R10", "SHADOW")));
        var tx = TestData.tx("tx-0001-abcdef", 5_000);
        var snapshot = TestData.snapshot(tx).withProfile(Optional.empty()).withLists(true, false);

        var evaluation = loader.current().orElseThrow().evaluate(snapshot.toContext());

        assertThat(evaluation.shadowHits()).extracting("ruleId").contains("R10");
        assertThat(evaluation.activeHits()).extracting("ruleId").doesNotContain("R10");
        assertThat(FeatureSnapshot.UNKNOWN).isEqualTo(-1L);
    }

    @Test
    @DisplayName("o conjunto trocado é atômico: quem já pegou o antigo continua com ele")
    void troca() {
        loader.apply(json(ruleSet(41)));
        var antigo = loader.current().orElseThrow();

        loader.apply(json(ruleSet(42)));

        assertThat(antigo.version()).isEqualTo(41);
        assertThat(loader.current().orElseThrow()).isNotSameAs(antigo);
    }
}
