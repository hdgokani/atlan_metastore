package org.apache.atlas.repository.graph.indexmanager;

import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collections;

import static org.apache.atlas.repository.Constants.EDGE_INDEX;
import static org.apache.atlas.repository.graph.indexmanager.IndexApplicabilityChecker.isIndexApplicable;

@Component
public class EdgeIndexCreator {

    private static final Logger LOG = LoggerFactory.getLogger(EdgeIndexCreator.class);

    public void createEdgeIndex(AtlasGraphManagement management, String propertyName, Class propertyClass,
                                AtlasCardinality cardinality, boolean createCompositeIndex) {
        if (propertyName != null) {
            AtlasPropertyKey propertyKey = management.getPropertyKey(propertyName);

            if (propertyKey == null) {
                propertyKey = management.makePropertyKey(propertyName, propertyClass, cardinality);

                if (isIndexApplicable(propertyClass, cardinality)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Creating backing index for edge property {} of type {} ", propertyName, propertyClass.getName());
                    }

                    management.addMixedIndex(EDGE_INDEX, propertyKey, false);

                    LOG.info("Created backing index for edge property {} of type {} ", propertyName, propertyClass.getName());
                }
            }

            if (propertyKey != null) {
                if (createCompositeIndex) {
                    createEdgeCompositeIndex(management, propertyClass, propertyKey);
                }
            } else {
                LOG.warn("Index not created for {}: propertyKey is null", propertyName);
            }
        }
    }

    private void createEdgeCompositeIndex(AtlasGraphManagement management, Class propertyClass, AtlasPropertyKey propertyKey) {
        String propertyName = propertyKey.getName();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating composite index for property {} of type {}", propertyName, propertyClass.getName());
        }

        AtlasGraphIndex existingIndex = management.getGraphIndex(propertyName);

        if (existingIndex == null) {
            management.createEdgeCompositeIndex(propertyName, false, Collections.singletonList(propertyKey));

            LOG.info("Created composite index for property {} of type {}", propertyName, propertyClass.getName());
        }
    }
}
