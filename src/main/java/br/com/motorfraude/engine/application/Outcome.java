package br.com.motorfraude.engine.application;

import br.com.motorfraude.engine.domain.Decision;

public record Outcome(boolean duplicate, Decision decision, boolean late) {

    public static Outcome duplicateOf() {
        return new Outcome(true, null, false);
    }

    public static Outcome decided(Decision decision, boolean late) {
        return new Outcome(false, decision, late);
    }
}
