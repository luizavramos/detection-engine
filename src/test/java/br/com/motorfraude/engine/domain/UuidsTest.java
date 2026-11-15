package br.com.motorfraude.engine.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class UuidsTest {

    private static final UUID DNS_NAMESPACE = UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8");

    @Test
    @DisplayName("bate com o vetor de referência da RFC 4122 (namespace DNS + www.example.com)")
    void vetorDeReferencia() {
        assertThat(Uuids.v5(DNS_NAMESPACE, "www.example.com")).hasToString("2ed6657d-e927-568b-95e1-2665a8aea6a2");
    }

    @Test
    @DisplayName("é estável e tem versão 5 e variante RFC 4122")
    void estavel() {
        var a = Uuids.v5(Uuids.NAMESPACE, "tx-0001-abcdef");
        var b = Uuids.v5(Uuids.NAMESPACE, "tx-0001-abcdef");

        assertThat(a).isEqualTo(b);
        assertThat(a.version()).isEqualTo(5);
        assertThat(a.variant()).isEqualTo(2);
        assertThat(Uuids.v5(Uuids.NAMESPACE, "tx-0002-abcdef")).isNotEqualTo(a);
    }
}
