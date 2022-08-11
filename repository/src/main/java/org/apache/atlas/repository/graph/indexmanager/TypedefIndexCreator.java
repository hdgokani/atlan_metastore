package org.apache.atlas.repository.graph.indexmanager;

import com.google.common.base.Preconditions;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.listener.ChangedTypeDefs;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.IndexException;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.IAtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.List;

@Component
public class TypedefIndexCreator extends GraphTransactionManager {

    private static final Logger LOG = LoggerFactory.getLogger(TypedefIndexCreator.class);

    private final IAtlasGraphProvider provider;
    private final AttributeIndexCreator attributeIndexCreator;
    private final AtlasTypeRegistry typeRegistry;
    private final IndexFieldNameResolver indexFieldNameResolver;

    @Inject
    public TypedefIndexCreator(IAtlasGraphProvider provider, AttributeIndexCreator attributeIndexCreator, AtlasTypeRegistry typeRegistry, IndexFieldNameResolver indexFieldNameResolver) {
        this.provider = provider;
        this.attributeIndexCreator = attributeIndexCreator;
        this.typeRegistry = typeRegistry;
        this.indexFieldNameResolver = indexFieldNameResolver;
    }


    public void createIndexForTypedefs(ChangedTypeDefs changedTypeDefs) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing changed typedefs {}", changedTypeDefs);
        }

        AtlasGraphManagement management = null;

        try {
            management = provider.get().getManagementSystem();

            // Update index for newly created types
            if (CollectionUtils.isNotEmpty(changedTypeDefs.getCreatedTypeDefs())) {
                for (AtlasBaseTypeDef typeDef : changedTypeDefs.getCreatedTypeDefs()) {
                    attributeIndexCreator.updateIndexForTypeDef(management, typeDef);
                }
            }

            // Update index for updated types
            if (CollectionUtils.isNotEmpty(changedTypeDefs.getUpdatedTypeDefs())) {
                for (AtlasBaseTypeDef typeDef : changedTypeDefs.getUpdatedTypeDefs()) {
                    attributeIndexCreator.updateIndexForTypeDef(management, typeDef);
                }
            }

            // Invalidate the property key for deleted types
            if (CollectionUtils.isNotEmpty(changedTypeDefs.getDeletedTypeDefs())) {
                for (AtlasBaseTypeDef typeDef : changedTypeDefs.getDeletedTypeDefs()) {
                    deleteIndexForType(management, typeDef);
                }
            }

            //resolve index fields names for the new entity attributes.
            indexFieldNameResolver.resolveIndexFieldNames(management, changedTypeDefs);

            createEdgeLabels(management, changedTypeDefs.getCreatedTypeDefs());
            createEdgeLabels(management, changedTypeDefs.getUpdatedTypeDefs());

            //Commit indexes
            commit(management);
        } catch (RepositoryException | IndexException e) {
            LOG.error("Failed to update indexes for changed typedefs", e);
            attemptRollback(changedTypeDefs, management);
        }

//        notifyChangeListeners(changedTypeDefs);
    }


    private void deleteIndexForType(AtlasGraphManagement management, AtlasBaseTypeDef typeDef) {
        Preconditions.checkNotNull(typeDef, "Cannot process null typedef");

        if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting indexes for type {}", typeDef.getName());
        }

        if (typeDef instanceof AtlasStructDef) {
            AtlasStructDef structDef = (AtlasStructDef) typeDef;
            List<AtlasStructDef.AtlasAttributeDef> attributeDefs = structDef.getAttributeDefs();

            if (CollectionUtils.isNotEmpty(attributeDefs)) {
                for (AtlasStructDef.AtlasAttributeDef attributeDef : attributeDefs) {
                    deleteIndexForAttribute(management, typeDef.getName(), attributeDef);
                }
            }
        }

        LOG.info("Completed deleting indexes for type {}", typeDef.getName());
    }

    private void deleteIndexForAttribute(AtlasGraphManagement management, String typeName, AtlasStructDef.AtlasAttributeDef attributeDef) {
        final String propertyName = AtlasGraphUtilsV2.encodePropertyKey(typeName + "." + attributeDef.getName());

        try {
            if (management.containsPropertyKey(propertyName)) {
                LOG.info("Deleting propertyKey {}, for attribute {}.{}", propertyName, typeName, attributeDef.getName());

                management.deletePropertyKey(propertyName);
            }
        } catch (Exception excp) {
            LOG.warn("Failed to delete propertyKey {}, for attribute {}.{}", propertyName, typeName, attributeDef.getName());
        }
    }

    private void createEdgeLabels(AtlasGraphManagement management, List<? extends AtlasBaseTypeDef> typeDefs) {
        if (CollectionUtils.isEmpty(typeDefs)) {
            return;
        }

        for (AtlasBaseTypeDef typeDef : typeDefs) {
            if (typeDef instanceof AtlasEntityDef) {
                AtlasEntityDef entityDef = (AtlasEntityDef) typeDef;
                createEdgeLabelsForStruct(management, entityDef);
            } else if (typeDef instanceof AtlasRelationshipDef) {
                createEdgeLabels(management, (AtlasRelationshipDef) typeDef);
            }
        }
    }

    private void createEdgeLabelsForStruct(AtlasGraphManagement management, AtlasEntityDef entityDef) {
        try {
            AtlasType type = typeRegistry.getType(entityDef.getName());
            if (!(type instanceof AtlasEntityType)) {
                return;
            }

            AtlasEntityType entityType = (AtlasEntityType) type;
            for (AtlasStructDef.AtlasAttributeDef attributeDef : entityDef.getAttributeDefs()) {
                AtlasStructType.AtlasAttribute attribute = entityType.getAttribute(attributeDef.getName());
                if (attribute.getAttributeType().getTypeCategory() == TypeCategory.STRUCT) {
                    String relationshipLabel = attribute.getRelationshipEdgeLabel();
                    createEdgeLabelUsingLabelName(management, relationshipLabel);
                }
            }
        } catch (AtlasBaseException e) {
            LOG.error("Error fetching type: {}", entityDef.getName(), e);
        }
    }

    private void createEdgeLabels(AtlasGraphManagement management, AtlasRelationshipDef relationshipDef) {
        String relationshipTypeName = relationshipDef.getName();
        AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName(relationshipTypeName);
        String relationshipLabel = relationshipType.getRelationshipLabel();

        createEdgeLabelUsingLabelName(management, relationshipLabel);
    }

    private void createEdgeLabelUsingLabelName(final AtlasGraphManagement management, final String label) {
        if (StringUtils.isEmpty(label)) {
            return;
        }

        org.apache.atlas.repository.graphdb.AtlasEdgeLabel edgeLabel = management.getEdgeLabel(label);

        if (edgeLabel == null) {
            management.makeEdgeLabel(label);

            LOG.info("Created edge label {} ", label);
        }
    }
}
