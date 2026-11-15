package br.com.motorfraude.engine.fakes;

import br.com.motorfraude.engine.application.gateway.RuleSetProvider;
import br.com.motorfraude.rules.kernel.compiler.CompiledRuleSet;
import java.util.Optional;

public final class FixedRuleSetProvider implements RuleSetProvider {

    private CompiledRuleSet current;

    public FixedRuleSetProvider(CompiledRuleSet current) {
        this.current = current;
    }

    public void set(CompiledRuleSet next) {
        this.current = next;
    }

    @Override
    public Optional<CompiledRuleSet> current() {
        return Optional.ofNullable(current);
    }
}
