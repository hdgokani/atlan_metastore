package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.model.instance.AtlasRelationship;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.apache.atlas.repository.Constants.INDEX_PREFIX;
import static org.apache.atlas.repository.Constants.VERTEX_INDEX;

public class AtlasNestedRelationshipsESQueryBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasNestedRelationshipsESQueryBuilder.class);
    private static final String INDEX_NAME = INDEX_PREFIX + VERTEX_INDEX;

    public static UpdateRequest getQueryForAppendingNestedRelationships(String docId, Map<String, Object> paramsMap) throws IOException {
        return new UpdateRequest().index(INDEX_NAME).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).id(docId)
                .script(new Script(
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
    }

    public static UpdateRequest getRelationshipDeletionQuery(AtlasRelationship relationship, String docId, Map<String, Object> params) {
        return new UpdateRequest().index(INDEX_NAME).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).id(docId)
                .script(new Script(
                        ScriptType.INLINE, "painless",
                        "if(ctx._source.containsKey('__relationships')){ctx._source.__relationships.removeIf(r -> r.relationshipGuid == params.relationshipGuid);}",
                        params
                ));
    }

}