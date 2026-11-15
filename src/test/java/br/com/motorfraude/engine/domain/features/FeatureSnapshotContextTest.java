package br.com.motorfraude.engine.domain.features;

import static br.com.motorfraude.engine.TestData.NOW;
import static br.com.motorfraude.engine.TestData.profile;
import static br.com.motorfraude.engine.TestData.snapshot;
import static br.com.motorfraude.engine.TestData.tx;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import br.com.motorfraude.contracts.Segment;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FeatureSnapshotContextTest {

    private final FeatureSnapshot base = snapshot(tx("tx-0001-abcdef", 150_000));

    @Test
    @DisplayName("perfil presente: segmento, idade da conta em dias e horas desde a troca de credencial, relativos ao instante da transação")
    void perfilPresente() {
        var snapshot = base.withProfile(Optional.of(profile(Segment.PJ, NOW.minus(Duration.ofDays(400)), NOW.minus(Duration.ofHours(2)))));

        assertThat(snapshot.values()).containsEntry("profile.segment", "PJ").containsEntry("profile.accountAgeDays", 400L)
                .containsEntry("profile.hoursSinceCredentialChange", 2L);
        assertThat(snapshot.profileKnown()).isTrue();
    }

    @Test
    @DisplayName("sem troca de credencial registrada, ou com troca depois da transação, o valor é -1 (as regras testam >= 0)")
    void semTroca() {
        var never = base.withProfile(Optional.of(profile(Segment.PF, NOW.minus(Duration.ofDays(10)), null)));
        var future = base.withProfile(Optional.of(profile(Segment.PF, NOW.minus(Duration.ofDays(10)), NOW.plus(Duration.ofHours(1)))));

        assertThat(never.values()).containsEntry("profile.hoursSinceCredentialChange", -1L);
        assertThat(future.values()).containsEntry("profile.hoursSinceCredentialChange", -1L);
    }

    @Test
    @DisplayName("perfil ausente: valores neutros (UNKNOWN, -1, -1) que as regras iniciais não confundem com sinal de risco")
    void perfilAusente() {
        var snapshot = base.withProfile(Optional.empty());

        assertThat(snapshot.values()).containsEntry("profile.segment", "UNKNOWN").containsEntry("profile.accountAgeDays", -1L)
                .containsEntry("profile.hoursSinceCredentialChange", -1L);
        assertThat(snapshot.profileKnown()).isFalse();
    }

    @Test
    @DisplayName("listas de bloqueio entram como lists.counterpartyFlagged e lists.deviceFlagged")
    void listas() {
        var snapshot = base.withLists(true, false);

        assertThat(snapshot.values()).containsEntry("lists.counterpartyFlagged", true).containsEntry("lists.deviceFlagged", false);
    }

    @Test
    @DisplayName("o contexto de avaliação só sai quando perfil e listas foram preenchidos")
    void contexto() {
        var complete = base.withProfile(Optional.empty()).withLists(false, false).toContext();

        assertThat(complete.values()).hasSize(22);
        assertThat(complete.segment()).isEqualTo("UNKNOWN");
        assertThatThrownBy(base::toContext).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("faltam");
    }

    @Test
    @DisplayName("os passos devolvem um instantâneo novo, sem alterar o anterior")
    void imutavel() {
        base.withLists(true, true);

        assertThat(base.values()).doesNotContainKey("lists.counterpartyFlagged");
    }
}
