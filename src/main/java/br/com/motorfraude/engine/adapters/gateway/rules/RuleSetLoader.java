package br.com.motorfraude.engine.adapters.gateway.rules;

import br.com.motorfraude.contracts.JsonCodec;
import br.com.motorfraude.contracts.RuleSet;
import br.com.motorfraude.contracts.Schemas;
import br.com.motorfraude.engine.application.gateway.RuleSetMetrics;
import br.com.motorfraude.rules.kernel.compiler.CompiledRuleSet;
import br.com.motorfraude.rules.kernel.compiler.RuleCompiler;
import br.com.motorfraude.rules.kernel.compiler.RuleSetCompilationException;
import br.com.motorfraude.rules.kernel.model.RuleDefinition;
import br.com.motorfraude.rules.kernel.model.RuleSetDefinition;
import br.com.motorfraude.rules.kernel.model.RuleStatus;
import br.com.motorfraude.rules.kernel.model.Scoring;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RuleSetLoader {

    public enum Result {
        LOADED, IGNORED, REJECTED
    }

    private static final Logger LOG = LoggerFactory.getLogger(RuleSetLoader.class);

    private final RuleSetMetrics metrics;
    private final RuleCompiler compiler = new RuleCompiler();
    private final AtomicReference<CompiledRuleSet> current = new AtomicReference<>();

    public RuleSetLoader(RuleSetMetrics metrics) {
        this.metrics = metrics;
    }

    public Optional<CompiledRuleSet> current() {
        return Optional.ofNullable(current.get());
    }

    public long version() {
        var set = current.get();
        return set == null ? 0 : set.version();
    }

    public Result apply(String json) {
        if (json == null) {
            return Result.IGNORED;
        }
        RuleSet message;
        try {
            var tree = JsonCodec.readTree(json);
            var errors = Schemas.validate("rule-set.v1", tree).errors();
            if (!errors.isEmpty()) {
                return rejected("invalid", "fora do contrato rule-set.v1: " + errors);
            }
            message = JsonCodec.fromTree(tree, RuleSet.class);
        } catch (RuntimeException e) {
            return rejected("invalid", "não foi possível ler o conjunto: " + e.getMessage());
        }

        if (message.version() <= version()) {
            metrics.ignored(message.version());
            return Result.IGNORED;
        }

        CompiledRuleSet compiled;
        try {
            compiled = compiler.compileAll(toDefinition(message));
        } catch (RuleSetCompilationException e) {
            return rejected("compile", e.getMessage());
        } catch (RuntimeException e) {
            return rejected("compile", "erro ao compilar a versão " + message.version() + ": " + e);
        }
        current.set(compiled);
        metrics.loaded(message.version(), message.publishedAt());
        LOG.info("conjunto de regras {} carregado ({} regras)", message.version(), message.rules().size());
        return Result.LOADED;
    }

    private Result rejected(String reason, String detail) {
        metrics.failed(reason);
        LOG.error("conjunto de regras rejeitado, mantida a versão {}: {}", version(), detail);
        return Result.REJECTED;
    }

    private static RuleSetDefinition toDefinition(RuleSet message) {
        var scoring = new Scoring(message.scoring().mediumThreshold(), message.scoring().highThreshold());
        var rules = message.rules().stream().map(rule -> new RuleDefinition(rule.ruleId(), rule.version(),
                RuleStatus.valueOf(rule.status().name()), rule.expression(), rule.params(), rule.paramOverrides(), rule.weight(),
                rule.reasonTemplate())).toList();
        return new RuleSetDefinition(message.version(), message.publishedAt(), scoring, rules);
    }
}
