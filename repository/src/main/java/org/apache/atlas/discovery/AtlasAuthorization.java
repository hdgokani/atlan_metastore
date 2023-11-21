package org.apache.atlas.discovery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AtlasAuthorization {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasAuthorization.class);

    private ObjectMapper mapper = new ObjectMapper();
    private String policiesString;
    private Map<String, Set> userPoliciesMap= new HashMap<>();

    public AtlasAuthorization () {
        try {
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
