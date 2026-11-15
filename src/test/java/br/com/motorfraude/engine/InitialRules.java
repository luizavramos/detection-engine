package br.com.motorfraude.engine;

import br.com.motorfraude.rules.kernel.compiler.CompiledRuleSet;
import br.com.motorfraude.rules.kernel.compiler.RuleCompiler;
import br.com.motorfraude.rules.kernel.model.RuleDefinition;
import br.com.motorfraude.rules.kernel.model.RuleSetDefinition;
import br.com.motorfraude.rules.kernel.model.RuleStatus;
import br.com.motorfraude.rules.kernel.model.Scoring;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public final class InitialRules {

    private static final JsonNode ROOT = load();

    private InitialRules() {
    }

    private static JsonNode load() {
        try (var in = InitialRules.class.getResourceAsStream("/initial-rules.json")) {
            return JsonMapper.builder().build().readTree(in);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static CompiledRuleSet compiled(long version) {
        return compiled(version, Map.of());
    }

    public static CompiledRuleSet compiled(long version, Map<String, RuleStatus> statusOverrides) {
        var scoring = new Scoring(ROOT.get("scoring").get("mediumThreshold").asInt(), ROOT.get("scoring").get("highThreshold").asInt());
        var rules = new ArrayList<RuleDefinition>();
        for (JsonNode json : ROOT.get("rules")) {
            var id = json.get("ruleId").asString();
            rules.add(rule(json, statusOverrides.getOrDefault(id, RuleStatus.ACTIVE)));
        }
        return new RuleCompiler().compileAll(new RuleSetDefinition(version, Instant.EPOCH, scoring, rules));
    }

    private static Object scalar(JsonNode node) {
        if (node.isBoolean()) {
            return node.asBoolean();
        }
        if (node.isIntegralNumber()) {
            return node.asLong();
        }
        return node.asString();
    }

    private static Map<String, Object> map(JsonNode node) {
        var result = new LinkedHashMap<String, Object>();
        if (node != null) {
            node.properties().forEach(e -> result.put(e.getKey(), scalar(e.getValue())));
        }
        return result;
    }

    private static RuleDefinition rule(JsonNode json, RuleStatus status) {
        var overrides = new LinkedHashMap<String, Map<String, Object>>();
        if (json.has("paramOverrides")) {
            json.get("paramOverrides").properties().forEach(e -> overrides.put(e.getKey(), map(e.getValue())));
        }
        return new RuleDefinition(json.get("ruleId").asString(), 1, status, json.get("expression").asString(),
                map(json.get("params")), overrides, json.get("weight").asInt(), json.get("reasonTemplate").asString());
    }
}
