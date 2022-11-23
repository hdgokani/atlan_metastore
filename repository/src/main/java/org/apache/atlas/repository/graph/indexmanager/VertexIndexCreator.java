package org.apache.atlas.repository.graph.indexmanager;

import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.atlas.repository.Constants.ENTITY_TYPE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.SUPER_TYPES_PROPERTY_KEY;
import static org.apache.atlas.repository.graph.indexmanager.IndexApplicabilityChecker.isIndexApplicable;
import static org.apache.atlas.repository.graphdb.AtlasCardinality.SET;
import static org.apache.atlas.repository.graphdb.AtlasCardinality.SINGLE;
import static org.apache.atlas.service.ActiveIndexNameManager.getCurrentWriteVertexIndexName;

@Component
public class VertexIndexCreator {

    private static final Logger LOG = LoggerFactory.getLogger(VertexIndexCreator.class);

    public String createVertexIndex(AtlasGraphManagement management, String propertyName, UniqueKind uniqueKind, Class propertyClass,
                                    AtlasCardinality cardinality, boolean createCompositeIndex, boolean createCompositeIndexWithTypeAndSuperTypes, boolean isStringField, HashMap<String, Object> indexTypeESConfig, HashMap<String, HashMap<String, Object>> indexTypeESFields) {
        String indexFieldName = null;

        if (propertyName != null) {
            boolean logP = propertyName.equals("recordUser");
            AtlasPropertyKey propertyKey = management.getPropertyKey(propertyName);

            if (propertyKey == null) {
                propertyKey = management.makePropertyKey(propertyName, propertyClass, cardinality);
                if (logP) {
                    LOG.info("propertyKey: {}", propertyKey.toString());
                }
            }

            if (logP) {
                LOG.info("getCurrentWriteVertexIndexName() {}", getCurrentWriteVertexIndexName());
                if (management.getGraphIndex(getCurrentWriteVertexIndexName()) != null) {
                    LOG.info("getFieldKeys() {}", management.getGraphIndex(getCurrentWriteVertexIndexName()).getFieldKeys());
                    LOG.info("propertyKey exists: {}", !management.getGraphIndex(getCurrentWriteVertexIndexName()).getFieldKeys().contains(propertyKey));
                }
            }

            if (isIndexApplicable(propertyClass, cardinality) && !management.getGraphIndex(getCurrentWriteVertexIndexName()).getFieldKeys().contains(propertyKey)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Creating backing index for vertex property {} of type {} ", propertyName, propertyClass.getName());
                }

                indexFieldName = management.addMixedIndex(getCurrentWriteVertexIndexName(), propertyKey, isStringField, indexTypeESConfig, indexTypeESFields);
                LOG.info("Created backing index for vertex property {} of type {} ", propertyName, propertyClass.getName());
            }

            if (indexFieldName == null && isIndexApplicable(propertyClass, cardinality)) {
                indexFieldName = management.getIndexFieldName(getCurrentWriteVertexIndexName(), propertyKey, isStringField);
            }

            if (propertyKey != null) {
                if (createCompositeIndex || uniqueKind == UniqueKind.GLOBAL_UNIQUE || uniqueKind == UniqueKind.PER_TYPE_UNIQUE) {
                    createVertexCompositeIndex(management, propertyClass, propertyKey, uniqueKind == UniqueKind.GLOBAL_UNIQUE);
                }

                if (createCompositeIndexWithTypeAndSuperTypes) {
                    createVertexCompositeIndexWithSystemProperty(management, propertyClass, propertyKey, ENTITY_TYPE_PROPERTY_KEY, SINGLE, uniqueKind == UniqueKind.PER_TYPE_UNIQUE);
                    createVertexCompositeIndexWithSystemProperty(management, propertyClass, propertyKey, SUPER_TYPES_PROPERTY_KEY, SET, false);
                }
            } else {
                LOG.warn("Index not created for {}: propertyKey is null", propertyName);
            }
        }

        return indexFieldName;
    }

    private void createVertexCompositeIndex(AtlasGraphManagement management, Class propertyClass, AtlasPropertyKey propertyKey,
                                            boolean enforceUniqueness) {
        String propertyName = propertyKey.getName();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating composite index for property {} of type {}; isUnique={} ", propertyName, propertyClass.getName(), enforceUniqueness);
        }

        AtlasGraphIndex existingIndex = management.getGraphIndex(propertyName);

        if (existingIndex == null) {
            management.createVertexCompositeIndex(propertyName, enforceUniqueness, Collections.singletonList(propertyKey));

            LOG.info("Created composite index for property {} of type {}; isUnique={} ", propertyName, propertyClass.getName(), enforceUniqueness);
        }
    }

    private void createVertexCompositeIndexWithSystemProperty(AtlasGraphManagement management, Class propertyClass, AtlasPropertyKey propertyKey,
                                                              final String systemPropertyKey, AtlasCardinality cardinality, boolean isUnique) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating composite index for property {} of type {} and {}", propertyKey.getName(), propertyClass.getName(), systemPropertyKey);
        }

        AtlasPropertyKey typePropertyKey = management.getPropertyKey(systemPropertyKey);
        if (typePropertyKey == null) {
            typePropertyKey = management.makePropertyKey(systemPropertyKey, String.class, cardinality);
        }

        final String indexName = propertyKey.getName() + systemPropertyKey;
        AtlasGraphIndex existingIndex = management.getGraphIndex(indexName);

        if (existingIndex == null) {
            List<AtlasPropertyKey> keys = new ArrayList<>(2);
            keys.add(typePropertyKey);
            keys.add(propertyKey);
            management.createVertexCompositeIndex(indexName, isUnique, keys);

            LOG.info("Created composite index for property {} of type {} and {}", propertyKey.getName(), propertyClass.getName(), systemPropertyKey);
        }
    }

}
