package br.com.motorfraude.engine.application.gateway;

import br.com.motorfraude.rules.kernel.compiler.CompiledRuleSet;
import java.util.Optional;

public interface RuleSetProvider {

    Optional<CompiledRuleSet> current();
}
