package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.model.instance.AtlasRelationship;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.repository.Constants.INDEX_PREFIX;
import static org.apache.atlas.repository.Constants.VERTEX_INDEX;

public class AtlasNestedRelationshipsQueryBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasNestedRelationshipsQueryBuilder.class);
    private static final String INDEX_NAME = INDEX_PREFIX + VERTEX_INDEX;
    private static final String GUID_KEY = "__guid";
    private static final String END2_TYPENAME = "__typeName";
    private static final String RELATIONSHIPS_PARAMS_KEY = "relationships";
    private static final String RELATIONSHIP_GUID_KEY = "relationshipGuid";
    private static final String RELATIONSHIPS_TYPENAME_KEY = "relationshipTypeName";

    public static UpdateByQueryRequest getQueryForAppendingNestedRelationships(String guid, Map<String, List<AtlasRelationship>> end1GuidRelationshipsMap) {
        UpdateByQueryRequest request = new UpdateByQueryRequest(INDEX_NAME);
        request.setQuery(new TermQueryBuilder(GUID_KEY, guid));
        Map<String, Object> paramsMap = getScriptParamsMap(end1GuidRelationshipsMap, guid);
        request.setScript(
                new Script(
                        ScriptType.INLINE, "painless",
                        "if(!ctx._source.containsKey('__relationships')) \n" +
                                "{ctx._source['__relationships']=[]}\n" +
                                "int size = ctx._source['__relationships'].size();\n" +
                                "ctx._source.__relationships.addAll(params.relationships);\n" +
                                "Map i = null;\n" +
                                "Set set = new HashSet();\n" +
                                "for(ListIterator it = ctx._source['__relationships'].listIterator(); it.hasNext();){\n" +
                                " i = it.next();\n" +
                                " it.remove();\n" +
                                " set.add(i);\n" +
                                "}\n" +
                                "ctx._source['__relationships'].addAll(set);",
                        paramsMap));
        return request;
    }

    public static UpdateByQueryRequest getRelationshipDeletionQuery(AtlasRelationship relationship) {
        UpdateByQueryRequest request = new UpdateByQueryRequest(INDEX_NAME);
        request.setQuery(new TermQueryBuilder(GUID_KEY, relationship.getEnd1().getGuid()));
        request.setScript(
                new Script(
                        ScriptType.INLINE, "painless",
                        "if(ctx._source.containsKey('__relationships')){ctx._source.__relationships.removeIf(r -> r.relationshipGuid == params.relationshipGuid);}",
                        new HashMap<String, Object>() {{
                            put(RELATIONSHIP_GUID_KEY, relationship.getGuid());
                        }}
                ));
        return request;
    }

    private static Map<String, Object> getScriptParamsMap(Map<String, List<AtlasRelationship>> end1GuidRelationshipsMap, String guid) {
        Map<String, Object> paramsMap = new HashMap<>();
        List<Map<String, String>> relationshipNestedPayloadList = buildParamsListForScript(end1GuidRelationshipsMap, guid);
        paramsMap.put(RELATIONSHIPS_PARAMS_KEY, relationshipNestedPayloadList);
        return paramsMap;
    }

    private static List<Map<String, String>> buildParamsListForScript(Map<String, List<AtlasRelationship>> end1GuidRelationshipsMap, String guid) {
        List<Map<String, String>> relationshipNestedPayloadList = new ArrayList<>();
        for (AtlasRelationship r : end1GuidRelationshipsMap.get(guid)) {
            Map<String, String> relationshipNestedPayload = buildNestedRelationshipDoc(r);
            relationshipNestedPayloadList.add(relationshipNestedPayload);
        }
        return relationshipNestedPayloadList;
    }

    private static Map<String, String> buildNestedRelationshipDoc(AtlasRelationship r) {
        return new HashMap<String, String>() {{
            put(RELATIONSHIP_GUID_KEY, r.getGuid());
            put(RELATIONSHIPS_TYPENAME_KEY, r.getTypeName());
            put(GUID_KEY, r.getEnd2().getGuid());
            put(END2_TYPENAME, r.getEnd2().getTypeName());
        }};
    }

}