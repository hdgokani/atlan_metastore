package org.apache.atlas.repository.graph.indexmanager;

import org.apache.atlas.repository.IndexException;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graphdb.*;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;

import static org.apache.atlas.repository.Constants.CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_TEXT_KEY;
import static org.apache.atlas.repository.Constants.CREATED_BY_KEY;
import static org.apache.atlas.repository.Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.GUID_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.HISTORICAL_GUID_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.IS_INCOMPLETE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.LABELS_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.MODIFIED_BY_KEY;
import static org.apache.atlas.repository.Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TIMESTAMP_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.indexmanager.IndexApplicabilityChecker.isIndexApplicable;
import static org.apache.atlas.repository.graphdb.AtlasCardinality.*;
import static org.apache.atlas.type.Constants.*;

@Component
public class GraphBackedIndexCreator extends GraphTransactionManager {

    private static final Logger LOG = LoggerFactory.getLogger(GraphBackedIndexCreator.class);

    private final AtlasTypeRegistry typeRegistry;
    private final VertexIndexCreator vertexIndexCreator;
    private final EdgeIndexCreator edgeIndexCreator;

    @Inject
    public GraphBackedIndexCreator(AtlasTypeRegistry typeRegistry, VertexIndexCreator vertexIndexCreator, EdgeIndexCreator edgeIndexCreator) {
        this.typeRegistry = typeRegistry;
        this.vertexIndexCreator = vertexIndexCreator;
        this.edgeIndexCreator = edgeIndexCreator;
    }


    public void createDefaultIndexes(AtlasGraph graph) throws RepositoryException, IndexException {
        AtlasGraphManagement management = graph.getManagementSystem();

        try {
            LOG.info("Creating indexes for graph.");

            if (management.getGraphIndex(VERTEX_INDEX) == null) {
                management.createVertexMixedIndex(VERTEX_INDEX, BACKING_INDEX, Collections.emptyList());

                LOG.info("Created index : {}", VERTEX_INDEX);
            }

            if (management.getGraphIndex(EDGE_INDEX) == null) {
                management.createEdgeMixedIndex(EDGE_INDEX, BACKING_INDEX, Collections.emptyList());

                LOG.info("Created index : {}", EDGE_INDEX);
            }

            if (management.getGraphIndex(FULLTEXT_INDEX) == null) {
                management.createFullTextMixedIndex(FULLTEXT_INDEX, BACKING_INDEX, Collections.emptyList());

                LOG.info("Created index : {}", FULLTEXT_INDEX);
            }

            HashMap<String, Object> ES_DATE_FIELD = new HashMap<>();
            ES_DATE_FIELD.put("type", "date");
            ES_DATE_FIELD.put("format", "epoch_millis");
            HashMap<String, HashMap<String, Object>> TIMESTAMP_MULTIFIELDS = new HashMap<>();
            TIMESTAMP_MULTIFIELDS.put("date", ES_DATE_FIELD);

            HashMap<String, Object> ES_KEYWORD_FIELD = new HashMap<>();
            ES_KEYWORD_FIELD.put("type", "keyword");
            ES_KEYWORD_FIELD.put("normalizer", "atlan_normalizer");
            HashMap<String, HashMap<String, Object>> KEYWORD_MULTIFIELD = new HashMap<>();
            KEYWORD_MULTIFIELD.put("keyword", ES_KEYWORD_FIELD);

            HashMap<String, Object> ES_ATLAN_TEXT_ANALYZER_CONFIG = new HashMap<>();
            ES_ATLAN_TEXT_ANALYZER_CONFIG.put("analyzer", "atlan_text_analyzer");

            HashMap<String, Object> ES_ATLAN_TEXT_COMMA_ANALYZER_CONFIG = new HashMap<>();
            ES_ATLAN_TEXT_COMMA_ANALYZER_CONFIG.put("analyzer", "atlan_text_comma_analyzer");

            // create vertex indexes
            createCommonVertexIndex(management, GUID_PROPERTY_KEY, UniqueKind.GLOBAL_UNIQUE, String.class, SINGLE, true, false, true, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, HISTORICAL_GUID_PROPERTY_KEY, UniqueKind.GLOBAL_UNIQUE, String.class, SINGLE, true, false, false, new HashMap<>(), new HashMap<>());

            createCommonVertexIndex(management, TYPENAME_PROPERTY_KEY, UniqueKind.GLOBAL_UNIQUE, String.class, SINGLE, true, false, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, TYPESERVICETYPE_PROPERTY_KEY, UniqueKind.NONE, String.class, SINGLE, true, false, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, VERTEX_TYPE_PROPERTY_KEY, UniqueKind.NONE, String.class, SINGLE, true, false, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, VERTEX_ID_IN_IMPORT_KEY, UniqueKind.NONE, Long.class, SINGLE, true, false, false, new HashMap<>(), new HashMap<>());

            createCommonVertexIndex(management, ENTITY_TYPE_PROPERTY_KEY, UniqueKind.NONE, String.class, SINGLE, true, false, false, new HashMap<>(), KEYWORD_MULTIFIELD);
            createCommonVertexIndex(management, SUPER_TYPES_PROPERTY_KEY, UniqueKind.NONE, String.class, SET, true, false, false, new HashMap<>(), KEYWORD_MULTIFIELD);
            createCommonVertexIndex(management, TIMESTAMP_PROPERTY_KEY, UniqueKind.NONE, Long.class, SINGLE, false, false, false, new HashMap<>(), TIMESTAMP_MULTIFIELDS);
            createCommonVertexIndex(management, MODIFICATION_TIMESTAMP_PROPERTY_KEY, UniqueKind.NONE, Long.class, SINGLE, false, false, false, new HashMap<>(), TIMESTAMP_MULTIFIELDS);
            createCommonVertexIndex(management, STATE_PROPERTY_KEY, UniqueKind.NONE, String.class, SINGLE, false, false, true, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, CREATED_BY_KEY, UniqueKind.NONE, String.class, SINGLE, false, false, true, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, CLASSIFICATION_TEXT_KEY, UniqueKind.NONE, String.class, SINGLE, false, false, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, MODIFIED_BY_KEY, UniqueKind.NONE, String.class, SINGLE, false, false, true, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, CLASSIFICATION_NAMES_KEY, UniqueKind.NONE, String.class, SINGLE, true, false, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, PROPAGATED_CLASSIFICATION_NAMES_KEY, UniqueKind.NONE, String.class, SINGLE, true, false, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, TRAIT_NAMES_PROPERTY_KEY, UniqueKind.NONE, String.class, SET, true, true, true, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, UniqueKind.NONE, String.class, LIST, true, true, true, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, PENDING_TASKS_PROPERTY_KEY, UniqueKind.NONE, String.class, SET, true, false, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, IS_INCOMPLETE_PROPERTY_KEY, UniqueKind.NONE, Integer.class, SINGLE, true, true, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, CUSTOM_ATTRIBUTES_PROPERTY_KEY, UniqueKind.NONE, String.class, SINGLE, true, false, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, LABELS_PROPERTY_KEY, UniqueKind.NONE, String.class, SINGLE, true, false, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, ENTITY_DELETED_TIMESTAMP_PROPERTY_KEY, UniqueKind.NONE, Long.class, SINGLE, true, false, false, new HashMap<>(), new HashMap<>());

            createCommonVertexIndex(management, PATCH_ID_PROPERTY_KEY, UniqueKind.GLOBAL_UNIQUE, String.class, SINGLE, true, false, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, PATCH_DESCRIPTION_PROPERTY_KEY, UniqueKind.NONE, String.class, SINGLE, true, false, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, PATCH_TYPE_PROPERTY_KEY, UniqueKind.NONE, String.class, SINGLE, true, false, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, PATCH_ACTION_PROPERTY_KEY, UniqueKind.NONE, String.class, SINGLE, true, false, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, PATCH_STATE_PROPERTY_KEY, UniqueKind.NONE, String.class, SINGLE, true, false, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, MEANINGS_PROPERTY_KEY, UniqueKind.NONE, String.class, SET, true, false, true, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, MEANINGS_TEXT_PROPERTY_KEY, UniqueKind.NONE, String.class, SINGLE, true, false, false, ES_ATLAN_TEXT_COMMA_ANALYZER_CONFIG, new HashMap<>());
            createCommonVertexIndex(management, MEANING_NAMES_PROPERTY_KEY, UniqueKind.NONE, String.class, LIST, true, false, true, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, GLOSSARY_PROPERTY_KEY, UniqueKind.NONE, String.class, SINGLE, true, false, true, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, CATEGORIES_PROPERTY_KEY, UniqueKind.NONE, String.class, SET, true, false, true, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, CATEGORIES_PARENT_PROPERTY_KEY, UniqueKind.NONE, String.class, SINGLE, true, false, true, new HashMap<>(), new HashMap<>());


            // tasks
            createCommonVertexIndex(management, TASK_GUID, UniqueKind.GLOBAL_UNIQUE, String.class, SINGLE, true, false, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, TASK_TYPE_PROPERTY_KEY, UniqueKind.NONE, String.class, SINGLE, true, false, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, TASK_CREATED_TIME, UniqueKind.NONE, Long.class, SINGLE, true, false, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, TASK_STATUS, UniqueKind.NONE, String.class, SINGLE, true, false, false, new HashMap<>(), new HashMap<>());

            createCommonVertexIndex(management, TASK_TYPE, UniqueKind.NONE, String.class, SINGLE, true, false, true, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, TASK_CREATED_BY, UniqueKind.NONE, String.class, SINGLE, false, false, true, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, TASK_ERROR_MESSAGE, UniqueKind.NONE, String.class, SINGLE, false, false, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, TASK_ATTEMPT_COUNT, UniqueKind.NONE, Integer.class, SINGLE, false, false, false, new HashMap<>(), new HashMap<>());

            createCommonVertexIndex(management, TASK_UPDATED_TIME, UniqueKind.NONE, Long.class, SINGLE, false, false, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, TASK_TIME_TAKEN_IN_SECONDS, UniqueKind.NONE, Long.class, SINGLE, false, false, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, TASK_START_TIME, UniqueKind.NONE, Long.class, SINGLE, false, false, false, new HashMap<>(), new HashMap<>());
            createCommonVertexIndex(management, TASK_END_TIME, UniqueKind.NONE, Long.class, SINGLE, false, false, false, new HashMap<>(), new HashMap<>());

            // index recovery
            createCommonVertexIndex(management, PROPERTY_KEY_INDEX_RECOVERY_NAME, UniqueKind.GLOBAL_UNIQUE, String.class, SINGLE, true, false, false, new HashMap<>(), new HashMap<>());

            // create vertex-centric index
            createVertexCentricIndex(management, CLASSIFICATION_LABEL, AtlasEdgeDirection.BOTH, CLASSIFICATION_EDGE_NAME_PROPERTY_KEY, String.class, SINGLE);
            createVertexCentricIndex(management, CLASSIFICATION_LABEL, AtlasEdgeDirection.BOTH, CLASSIFICATION_EDGE_IS_PROPAGATED_PROPERTY_KEY, Boolean.class, SINGLE);
            createVertexCentricIndex(management, CLASSIFICATION_LABEL, AtlasEdgeDirection.BOTH, Arrays.asList(CLASSIFICATION_EDGE_NAME_PROPERTY_KEY, CLASSIFICATION_EDGE_IS_PROPAGATED_PROPERTY_KEY));

            // create edge indexes

            edgeIndexCreator.createEdgeIndex(management, RELATIONSHIP_GUID_PROPERTY_KEY, String.class, SINGLE, true);

            edgeIndexCreator.createEdgeIndex(management, EDGE_ID_IN_IMPORT_KEY, String.class, SINGLE, true);

            edgeIndexCreator.createEdgeIndex(management, ATTRIBUTE_INDEX_PROPERTY_KEY, Integer.class, SINGLE, true);

            // create fulltext indexes
            createFullTextIndex(management, ENTITY_TEXT_PROPERTY_KEY, String.class, SINGLE);

            createPropertyKey(management, IS_PROXY_KEY, Boolean.class, SINGLE);
            createPropertyKey(management, PROVENANCE_TYPE_KEY, Integer.class, SINGLE);
            createPropertyKey(management, HOME_ID_KEY, String.class, SINGLE);

            commit(management);

            LOG.info("Index creation for global keys complete.");
        } catch (Throwable t) {
            LOG.error("GraphBackedSearchIndexer.initialize() failed", t);

            rollback(management);
            throw new RepositoryException(t);
        }
    }

    private void createCommonVertexIndex(AtlasGraphManagement management,
                                         String propertyName,
                                         UniqueKind uniqueKind,
                                         Class propertyClass,
                                         AtlasCardinality cardinality,
                                         boolean createCompositeIndex,
                                         boolean createCompositeIndexWithTypeAndSuperTypes,
                                         boolean isStringField, HashMap<String, Object> indexTypeESConfig, HashMap<String, HashMap<String, Object>> indexTypeESFields) {
        if (isStringField && String.class.equals(propertyClass)) {

            propertyName = AtlasStructType.AtlasAttribute.VERTEX_PROPERTY_PREFIX_STRING_INDEX_TYPE + propertyName;
            LOG.debug("Creating the common attribute '{}' as string field.", propertyName);
        }

        final String indexFieldName = vertexIndexCreator.createVertexIndex(
                management,
                propertyName,
                uniqueKind,
                propertyClass,
                cardinality,
                createCompositeIndex,
                createCompositeIndexWithTypeAndSuperTypes,
                isStringField,
                indexTypeESConfig,
                indexTypeESFields
        );
        if (indexFieldName != null) {
            typeRegistry.addIndexFieldName(propertyName, indexFieldName);
        }
    }

    private void createVertexCentricIndex(AtlasGraphManagement management, String edgeLabel, AtlasEdgeDirection edgeDirection,
                                          String propertyName, Class propertyClass, AtlasCardinality cardinality) {
        AtlasPropertyKey propertyKey = management.getPropertyKey(propertyName);

        if (propertyKey == null) {
            propertyKey = management.makePropertyKey(propertyName, propertyClass, cardinality);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating vertex-centric index for edge label: {} direction: {} for property: {} of type: {} ",
                    edgeLabel, edgeDirection.name(), propertyName, propertyClass.getName());
        }

        final String indexName = edgeLabel + propertyKey.getName();

        if (!management.edgeIndexExist(edgeLabel, indexName)) {
            management.createEdgeIndex(edgeLabel, indexName, edgeDirection, Collections.singletonList(propertyKey));

            LOG.info("Created vertex-centric index for edge label: {} direction: {} for property: {} of type: {}",
                    edgeLabel, edgeDirection.name(), propertyName, propertyClass.getName());
        }
    }

    private void createVertexCentricIndex(AtlasGraphManagement management, String edgeLabel, AtlasEdgeDirection edgeDirection, List<String> propertyNames) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating vertex-centric index for edge label: {} direction: {} for properties: {}",
                    edgeLabel, edgeDirection.name(), propertyNames);
        }

        String indexName = edgeLabel;
        List<AtlasPropertyKey> propertyKeys = new ArrayList<>();

        for (String propertyName : propertyNames) {
            AtlasPropertyKey propertyKey = management.getPropertyKey(propertyName);

            if (propertyKey != null) {
                propertyKeys.add(propertyKey);
                indexName = indexName + propertyKey.getName();
            }
        }

        if (!management.edgeIndexExist(edgeLabel, indexName) && CollectionUtils.isNotEmpty(propertyKeys)) {
            management.createEdgeIndex(edgeLabel, indexName, edgeDirection, propertyKeys);

            LOG.info("Created vertex-centric index for edge label: {} direction: {} for properties: {}", edgeLabel, edgeDirection.name(), propertyNames);
        }
    }

    private AtlasPropertyKey createFullTextIndex(AtlasGraphManagement management, String propertyName, Class propertyClass,
                                                 AtlasCardinality cardinality) {
        AtlasPropertyKey propertyKey = management.getPropertyKey(propertyName);

        if (propertyKey == null) {
            propertyKey = management.makePropertyKey(propertyName, propertyClass, cardinality);

            if (isIndexApplicable(propertyClass, cardinality)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Creating backing index for vertex property {} of type {} ", propertyName, propertyClass.getName());
                }

                management.addMixedIndex(FULLTEXT_INDEX, propertyKey, false);

                LOG.info("Created backing index for vertex property {} of type {} ", propertyName, propertyClass.getName());
            }

            LOG.info("Created index {}", FULLTEXT_INDEX);
        }

        return propertyKey;
    }

    private AtlasPropertyKey createPropertyKey(AtlasGraphManagement management, String propertyName, Class propertyClass, AtlasCardinality cardinality) {
        AtlasPropertyKey propertyKey = management.getPropertyKey(propertyName);

        if (propertyKey == null) {
            propertyKey = management.makePropertyKey(propertyName, propertyClass, cardinality);
        }

        return propertyKey;
    }
}
