package br.com.motorfraude.engine.application;

import br.com.motorfraude.engine.domain.Decision;

public record Outcome(boolean duplicate, Decision decision) {

    public static Outcome duplicateOf() {
        return new Outcome(true, null);
    }

    public static Outcome decided(Decision decision) {
        return new Outcome(false, decision);
    }
}
