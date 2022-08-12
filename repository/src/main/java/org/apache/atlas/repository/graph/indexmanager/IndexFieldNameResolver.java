package org.apache.atlas.repository.graph.indexmanager;

import org.apache.atlas.listener.ChangedTypeDefs;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.type.AtlasBusinessMetadataType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.List;

import static org.apache.atlas.repository.graph.indexmanager.AtlasCardinalityMapper.toAtlasCardinality;
import static org.apache.atlas.repository.graph.indexmanager.IndexApplicabilityChecker.isIndexApplicable;
import static org.apache.atlas.repository.graph.indexmanager.PrimitiveClassMapper.getPrimitiveClass;
import static org.apache.atlas.service.ActiveIndexNameManager.getCurrentIndexName;

@Component
public class IndexFieldNameResolver {

    private static final Logger LOG = LoggerFactory.getLogger(IndexFieldNameResolver.class);

    private final AtlasTypeRegistry typeRegistry;

    @Inject
    public IndexFieldNameResolver(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    public void resolveIndexFieldNames(AtlasGraphManagement managementSystem, ChangedTypeDefs changedTypeDefs) {
        List<? extends AtlasBaseTypeDef> createdTypeDefs = changedTypeDefs.getCreatedTypeDefs();

        if (createdTypeDefs != null) {
            resolveIndexFieldNames(managementSystem, createdTypeDefs);
        }

        List<? extends AtlasBaseTypeDef> updatedTypeDefs = changedTypeDefs.getUpdatedTypeDefs();

        if (updatedTypeDefs != null) {
            resolveIndexFieldNames(managementSystem, updatedTypeDefs);
        }
    }

    private void resolveIndexFieldNames(AtlasGraphManagement managementSystem, List<? extends AtlasBaseTypeDef> typeDefs) {
        for (AtlasBaseTypeDef baseTypeDef : typeDefs) {
            if (TypeCategory.ENTITY.equals(baseTypeDef.getCategory())) {
                AtlasEntityType entityType = typeRegistry.getEntityTypeByName(baseTypeDef.getName());

                resolveIndexFieldNames(managementSystem, entityType);
            } else if (TypeCategory.BUSINESS_METADATA.equals(baseTypeDef.getCategory())) {
                AtlasBusinessMetadataType businessMetadataType = typeRegistry.getBusinessMetadataTypeByName(baseTypeDef.getName());

                resolveIndexFieldNames(managementSystem, businessMetadataType);
            } else {
                LOG.debug("Ignoring type definition {}", baseTypeDef.getName());
            }
        }
    }

    private void resolveIndexFieldNames(AtlasGraphManagement managementSystem, AtlasStructType structType) {
        for (AtlasStructType.AtlasAttribute attribute : structType.getAllAttributes().values()) {
            resolveIndexFieldName(managementSystem, attribute);
        }
    }

    private void resolveIndexFieldName(AtlasGraphManagement managementSystem, AtlasStructType.AtlasAttribute attribute) {
        try {
            if (attribute.getIndexFieldName() == null && TypeCategory.PRIMITIVE.equals(attribute.getAttributeType().getTypeCategory())) {
                AtlasStructType definedInType = attribute.getDefinedInType();
                AtlasStructType.AtlasAttribute baseInstance = definedInType != null ? definedInType.getAttribute(attribute.getName()) : null;

                if (baseInstance != null && baseInstance.getIndexFieldName() != null) {
                    attribute.setIndexFieldName(baseInstance.getIndexFieldName());
                } else if (isIndexApplicable(getPrimitiveClass(attribute.getTypeName()), toAtlasCardinality(attribute.getAttributeDef().getCardinality()))) {
                    AtlasPropertyKey propertyKey = managementSystem.getPropertyKey(attribute.getVertexPropertyName());
                    boolean isStringField = AtlasStructDef.AtlasAttributeDef.IndexType.STRING.equals(attribute.getIndexType());
                    if (propertyKey != null) {
                        String indexFieldName = managementSystem.getIndexFieldName(getCurrentIndexName(), propertyKey, isStringField);

                        attribute.setIndexFieldName(indexFieldName);

                        if (baseInstance != null) {
                            baseInstance.setIndexFieldName(indexFieldName);
                        }

                        typeRegistry.addIndexFieldName(attribute.getVertexPropertyName(), indexFieldName);

                        LOG.info("Property {} is mapped to index field name {}", attribute.getQualifiedName(), attribute.getIndexFieldName());
                    } else {
                        LOG.warn("resolveIndexFieldName(attribute={}): propertyKey is null for vertextPropertyName={}", attribute.getQualifiedName(), attribute.getVertexPropertyName());
                    }
                }
            }
        } catch (Exception excp) {
            LOG.warn("resolveIndexFieldName(attribute={}) failed.", attribute.getQualifiedName(), excp);
        }
    }
}
