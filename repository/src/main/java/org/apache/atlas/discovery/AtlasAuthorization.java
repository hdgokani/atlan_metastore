package org.apache.atlas.discovery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchQuery;
import org.apache.commons.lang.StringUtils;

import static org.apache.atlas.repository.Constants.POLICY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase.getLowLevelClient;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import org.apache.kafka.common.protocol.types.Field;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AtlasAuthorization {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasAuthorization.class);

    private ObjectMapper mapper = new ObjectMapper();
    private String policiesString;
    private Map<String, Set> userPoliciesMap= new HashMap<>();
    private EntityDiscoveryService discoveryService;

    public AtlasAuthorization (EntityDiscoveryService discoveryService) {
        try {
            this.discoveryService = discoveryService;
            policiesString = getPolicies();
            createUserPolicyMap();
        } catch (Exception e) {
            LOG.info("==> AtlasAuthorization -> Error");
        }
    }

    private void createUserPolicyMap() {
        try {
            JsonNode jsonTree = mapper.readTree(policiesString);
            int treeSize = jsonTree.size();
            for (int i = 0; i < treeSize; i++) {
                JsonNode node = jsonTree.get(i);
                String users = node.get("properties").get("subjects").get("properties").get("users").get("items").get("pattern").asText();
                List<String> usersList = Arrays.asList(users.split("\\|"));
                for (String user : usersList) {
                    if (userPoliciesMap.get(user) == null) {
                        Set<Integer> policySet = new HashSet<>();
                        policySet.add(i);
                        userPoliciesMap.put(user, policySet);
                    } else {
                        userPoliciesMap.get(user).add(i);
                    }
                }
                LOG.info("userPoliciesMap");
                LOG.info(String.valueOf(userPoliciesMap));
            }
        } catch (Exception e) {
            LOG.error("Error -> createUserPolicyMap!", e);
        }
    }

    private String getPolicies() throws IOException {
        String atlasHomeDir = System.getProperty("atlas.home");
        String atlasHome = StringUtils.isEmpty(atlasHomeDir) ? "." : atlasHomeDir;
        File policiesFile = Paths.get(atlasHome, "policies", "jpolicies.json").toFile();
        return new String(Files.readAllBytes(policiesFile.toPath()), StandardCharsets.UTF_8);
    }

    protected JsonNode getJsonNodeFromStringContent(String content) throws IOException {
        return mapper.readTree(content);
    }

    protected JsonSchema getJsonSchemaFromJsonNode(JsonNode jsonNode) {
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4);
        return factory.getSchema(jsonNode);
    }

    protected com.networknt.schema.JsonSchema getJsonSchemaFromStringContent(String schemaContent) {
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4);
        return factory.getSchema(schemaContent);
    }

    private String jsonSchemaToElasticsearchDSL(String jsonSchema) {
        // Create ObjectMapper
        ObjectMapper objectMapper = new ObjectMapper();

        // Parse JSON 2 string
        JsonNode jsonSchemaNode = null;
        try {
            jsonSchemaNode = objectMapper.readTree(jsonSchema);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        // Create JSON 1 structure
        ObjectNode elasticsearchDSL = objectMapper.createObjectNode();
        ObjectNode elasticsearchDSLBool = objectMapper.createObjectNode();
        elasticsearchDSL.set("bool", elasticsearchDSLBool);
        ArrayNode mustNotNode = elasticsearchDSLBool.putArray("must_not");
        ArrayNode filterNode = elasticsearchDSLBool.putArray("filter");

        // Iterate over attributes in JSON 2 and convert to JSON 1
        jsonSchemaNode.fields().forEachRemaining(entry -> {
            String attributeName = entry.getKey();
            JsonNode attributeNode = entry.getValue();

            // Create condition based on attribute type and pattern
            if (attributeNode.has("pattern")) {
                String pattern = attributeNode.get("pattern").asText();
                if (pattern.startsWith("^(?!") && pattern.endsWith("$).*$")) {
                    // Convert to must_not condition
                    ObjectNode termNode = mustNotNode.addObject();
                    termNode.putObject("term").put(attributeName, pattern.substring(4, pattern.length() - 5));
                } else if (pattern.startsWith("^") && pattern.endsWith("$")) {
                    // Convert to filter condition
                    ObjectNode termNode = filterNode.addObject();
                    termNode.putObject("term").put(attributeName, pattern.substring(1, pattern.length() - 1));
                } else if (pattern.startsWith(".*")) {
                    // Convert to filter wildcard condition
                    ObjectNode wildcardNode = filterNode.addObject().putObject("wildcard");
                    wildcardNode.putObject(attributeName).put("value","*" + pattern.substring(2));
                } else if (pattern.endsWith(".*")) {
                    // Convert to filter wildcard condition
                    ObjectNode wildcardNode = filterNode.addObject().putObject("wildcard");
                    wildcardNode.putObject(attributeName).put("value", pattern.substring(0, pattern.length() - 2) + "*");
                }
            }
        });

        // Convert JSON 1 to string
        return elasticsearchDSL.toString();
    }

    private Map<String, Object> getMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    private String getAccessControlDSL(List<String> policyDSLArray, String entityQualifiedName, String entityTypeName) {
        try {
            List<Map<String, Object>> mustClauseList = new ArrayList<>();
            Map<String, List> boolObjects = new HashMap<>();
            List<Map<String, Object>> filterClauseList = new ArrayList<>();
            filterClauseList.add(getMap("term", getMap("qualifiedName", entityQualifiedName)));
            filterClauseList.add(getMap("term", getMap("__typeName.keyword", entityTypeName)));
            boolObjects.put("filter", filterClauseList);
            mustClauseList.add(getMap("bool", boolObjects));

            List<Map<String, Object>> shouldClauseList = new ArrayList<>();
            for (String policyDSL: policyDSLArray) {
                String policyDSLBase64 = Base64.getEncoder().encodeToString(policyDSL.getBytes());;
                shouldClauseList.add(getMap("wrapper", getMap("query", policyDSLBase64)));
            }
            Map<String, Object> boolClause = new HashMap<>();
            boolClause.put("should", shouldClauseList);
            boolClause.put("minimum_should_match", 1);
            mustClauseList.add(getMap("bool", boolClause));
            JsonNode queryNode = mapper.valueToTree(getMap("query", getMap("bool", getMap("must", mustClauseList))));
            return queryNode.toString();

        } catch (Exception e) {
            LOG.error("Error -> addPreFiltersToSearchQuery!", e);
            return null;
        }
    }

    private Integer getCountFromElasticsearch(String query) {
        RestClient restClient = getLowLevelClient();
        AtlasElasticsearchQuery elasticsearchQuery = new AtlasElasticsearchQuery("janusgraph_vertex_index", restClient);
        Map<String, Object> elasticsearchResult = null;
        try {
            elasticsearchResult = elasticsearchQuery.runQueryWithLowLevelClient(query);
        } catch (AtlasBaseException e) {
            e.printStackTrace();
        }
        Integer count = null;
        if (elasticsearchResult!=null) {
            count = (Integer) elasticsearchResult.get("total");
        }
        return count;
    }
    private List<AtlasEntityHeader> getRelevantPolicies(String user, String action) throws AtlasBaseException {
        List<AtlasEntityHeader> ret = new ArrayList<>();

        IndexSearchParams indexSearchParams = new IndexSearchParams();
        Map<String, Object> dsl = new HashMap<>();

        List mustClauseList = new ArrayList();
        mustClauseList.add(mapOf("term", getMap("__typeName.keyword", POLICY_ENTITY_TYPE)));
        mustClauseList.add(mapOf("term", getMap("policyUsers", user)));
        mustClauseList.add(mapOf("term", getMap("policyActions", action)));

        dsl.put("query", mapOf("bool", mapOf("must", mustClauseList)));

        indexSearchParams.setDsl(dsl);
        Set<String> attributes = new HashSet<>();
        attributes.add("policyFilterCriteria");
        indexSearchParams.setAttributes(attributes);
        indexSearchParams.setSuppressLogs(true);

        AtlasSearchResult result = discoveryService.directIndexSearch(indexSearchParams);
        if (result != null) {
            ret = result.getEntities();
        }

        return ret;
    }

    private List<String> getPolicyFilterCriteriaArray(List<AtlasEntityHeader> entityHeaders) {
        List<String> policyFilterCriteriaArray = new ArrayList<>();
        if (entityHeaders != null) {
            for (AtlasEntityHeader entity: entityHeaders) {
                String policyFilterCriteria = (String) entity.getAttribute("policyFilterCriteria");
                if (StringUtils.isNotEmpty(policyFilterCriteria)) {
                    policyFilterCriteriaArray.add(policyFilterCriteria);
                }
            }
        }
        return policyFilterCriteriaArray;
    }

    private List<String> getPolicyDSLArray(List<String> policyFilterCriteriaArray) {
        List<String> policyDSLArray = new ArrayList<>();
        for (String policyFilterCriteria: policyFilterCriteriaArray) {
            String policyDSL = jsonSchemaToElasticsearchDSL(policyFilterCriteria);
            policyDSLArray.add(policyDSL);
        }
        return policyDSLArray;
    }

    public List<Map<String, Object>> getDSLNodeForIndexsearch(String user, String action){
        List<AtlasEntityHeader> policies = new ArrayList<>();
        try {
            policies = getRelevantPolicies(user, action);
        } catch (AtlasBaseException e) {
            e.printStackTrace();
        }
        List<String> policyFilterCriteriaArray = getPolicyFilterCriteriaArray(policies);
        List<String> policyDSLArray = getPolicyDSLArray(policyFilterCriteriaArray);
        List<Map<String, Object>> shouldClauseList = new ArrayList<>();
        for (String policyDSL: policyDSLArray) {
            String policyDSLBase64 = Base64.getEncoder().encodeToString(policyDSL.getBytes());;
            shouldClauseList.add(getMap("wrapper", getMap("query", policyDSLBase64)));
        }
        return shouldClauseList;
    }

    public boolean isAccessAllowed(String user, String action, String entityQualifiedName, String entityTypeName){
        List<AtlasEntityHeader> policies = new ArrayList<>();
        Integer count = null;
        try {
            policies = getRelevantPolicies(user, action);
        } catch (AtlasBaseException e) {
            e.printStackTrace();
        }
        List<String> policyFilterCriteriaArray = getPolicyFilterCriteriaArray(policies);
        List<String> policyDSLArray = getPolicyDSLArray(policyFilterCriteriaArray);
        if (!policyDSLArray.isEmpty()) {
            String query = getAccessControlDSL(policyDSLArray, entityQualifiedName, entityTypeName);
            count = getCountFromElasticsearch(query);
        }

        if (count != null && count > 0) {
            return true;
        }
        return false;
    }

    public boolean isAccessAllowed(Map<String, Object> entity, String user){
        try {
            long startTime = System.nanoTime();
            JsonNode jsonTree = mapper.readTree(policiesString);
            String entityString = mapper.writeValueAsString(entity);
            JsonNode node = getJsonNodeFromStringContent(entityString);
            Set<Integer> userPolicies = userPoliciesMap.get(user);
            boolean error = false;
            for (Integer policyId : userPolicies) {
                JsonNode jsonNode = jsonTree.get(policyId);
                JsonSchema schema = getJsonSchemaFromJsonNode(jsonNode);
                Set<ValidationMessage> errors = schema.validate(node);
                if (errors != null && errors.size() > 0) {
                    error = true;
                }
                LOG.info("Error Count -> ");
                LOG.info(String.valueOf(errors.size()));
            }
            long endTime = System.nanoTime();
            long finalDuration = ((endTime - startTime)/1000000);
            LOG.info("finalDuration");
            LOG.info(String.valueOf(finalDuration));
            return userPolicies.size() > 0 &&  !error;
        } catch (Exception e) {
            LOG.info("error!");
            return false;
        }

    }

}
