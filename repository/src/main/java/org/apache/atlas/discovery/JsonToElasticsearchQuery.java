package org.apache.atlas.discovery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonToElasticsearchQuery {

    public static JsonNode convertConditionToQuery(String condition, JsonNode criterion, ObjectMapper mapper) {
        if (condition.equals("AND")) {
            return mapper.createObjectNode().set("bool", mapper.createObjectNode().set("filter", mapper.createArrayNode()));
        } else if (condition.equals("OR")) {
            JsonNode node = mapper.createObjectNode().set("bool", mapper.createObjectNode());
            return mapper.createObjectNode().set("bool", mapper.createObjectNode().set("should", mapper.createArrayNode()));
        } else {
            throw new IllegalArgumentException("Unsupported condition: " + condition);
        }
    }

    public static JsonNode convertJsonToQuery(JsonNode data, ObjectMapper mapper) {
        String condition = data.get("condition").asText();
        JsonNode criterion = data.get("criterion");

        JsonNode query = convertConditionToQuery(condition, criterion, mapper);

        for (JsonNode crit : criterion) {
            if (crit.has("condition")) {
                JsonNode nestedQuery = convertJsonToQuery(crit, mapper);
                if (condition.equals("AND")) {
                    ((com.fasterxml.jackson.databind.node.ArrayNode) query.get("bool").get("filter")).add(nestedQuery);
                } else {
                    ((com.fasterxml.jackson.databind.node.ArrayNode) query.get("bool").get("should")).add(nestedQuery);
                }
            } else {
                String operator = crit.get("operator").asText();
                String attributeName = crit.get("attributeName").asText();
                String attributeValue = crit.get("attributeValue").asText();

                if (operator.equals("EQUALS")) {
                    com.fasterxml.jackson.databind.node.ObjectNode termNode = ((com.fasterxml.jackson.databind.node.ArrayNode) query.get("bool").get(condition.equals("AND") ? "filter" : "should")).addObject();
                    termNode.putObject("term").put(attributeName, attributeValue);
                } else if (operator.equals("NOT_EQUALS")) {
                    com.fasterxml.jackson.databind.node.ObjectNode termNode = ((com.fasterxml.jackson.databind.node.ArrayNode) query.get("bool").get(condition.equals("AND") ? "filter" : "should")).addObject();
                    termNode.putObject("bool").putObject("must_not").putObject("term").put(attributeName, attributeValue);
                } else if (operator.equals("STARTS_WITH")) {
                    com.fasterxml.jackson.databind.node.ObjectNode wildcardNode = ((com.fasterxml.jackson.databind.node.ArrayNode) query.get("bool").get(condition.equals("AND") ? "filter" : "should")).addObject();
                    wildcardNode.putObject("wildcard").put(attributeName, attributeValue + "*");
                } else if (operator.equals("ENDS_WITH")) {
                    com.fasterxml.jackson.databind.node.ObjectNode wildcardNode = ((com.fasterxml.jackson.databind.node.ArrayNode) query.get("bool").get(condition.equals("AND") ? "filter" : "should")).addObject();
                    wildcardNode.putObject("wildcard").put(attributeName, "*" + attributeValue);
                } else if (operator.equals("IN")) {
                    com.fasterxml.jackson.databind.node.ObjectNode termsNode = ((com.fasterxml.jackson.databind.node.ArrayNode) query.get("bool").get(condition.equals("AND") ? "filter" : "should")).addObject();
                    termsNode.putObject("terms").set(attributeName, crit.get("attributeValue"));
                } else if (operator.equals("NOT_IN")) {
                    com.fasterxml.jackson.databind.node.ObjectNode termsNode = ((com.fasterxml.jackson.databind.node.ArrayNode) query.get("bool").get(condition.equals("AND") ? "filter" : "should")).addObject();
                    termsNode.putObject("bool").putObject("must_not").putObject("terms").put(attributeName, crit.get("attributeValue"));
                }

            }
        }

        return query;
    }
}
