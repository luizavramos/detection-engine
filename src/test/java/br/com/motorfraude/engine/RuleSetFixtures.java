package br.com.motorfraude.engine;

import java.io.IOException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

public final class RuleSetFixtures {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private RuleSetFixtures() {
    }

    public static ObjectNode ruleSet(long version) {
        try (var in = RuleSetFixtures.class.getResourceAsStream("/initial-rules.json")) {
            ObjectNode root = (ObjectNode) MAPPER.readTree(in);
            root.put("version", version);
            root.put("publishedAt", "2026-11-15T12:00:00Z");
            for (JsonNode node : root.get("rules")) {
                var rule = (ObjectNode) node;
                rule.remove("cases");
                rule.put("version", 1);
                rule.put("status", "ACTIVE");
                if (!rule.has("paramOverrides")) {
                    rule.putObject("paramOverrides");
                }
            }
            return root;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static ObjectNode withRule(ObjectNode set, String ruleId, String expression, int weight, String status) {
        var rule = ((ArrayNode) set.get("rules")).addObject();
        rule.put("ruleId", ruleId);
        rule.put("version", 1);
        rule.put("status", status);
        rule.put("expression", expression);
        rule.putObject("params");
        rule.putObject("paramOverrides");
        rule.put("weight", weight);
        rule.put("reasonTemplate", "Regra " + ruleId + " disparou");
        return set;
    }

    public static ObjectNode withBrokenRule(ObjectNode set) {
        return withRule(set, "R99", "features.naoExiste > 1", 10, "ACTIVE");
    }

    public static ObjectNode withStatus(ObjectNode set, String ruleId, String status) {
        for (JsonNode node : set.get("rules")) {
            if (node.get("ruleId").asString().equals(ruleId)) {
                ((ObjectNode) node).put("status", status);
            }
        }
        return set;
    }

    public static ObjectNode withParam(ObjectNode set, String ruleId, String param, long value) {
        for (JsonNode node : set.get("rules")) {
            if (node.get("ruleId").asString().equals(ruleId)) {
                ((ObjectNode) node.get("params")).put(param, value);
            }
        }
        return set;
    }

    public static ObjectNode without(ObjectNode set, String ruleId) {
        var rules = (ArrayNode) set.get("rules");
        for (int i = rules.size() - 1; i >= 0; i--) {
            if (rules.get(i).get("ruleId").asString().equals(ruleId)) {
                rules.remove(i);
            }
        }
        return set;
    }

    public static String json(ObjectNode set) {
        return set.toString();
    }
}
