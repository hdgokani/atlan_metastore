package org.apache.atlas.repository.graph.indexmanager;

import com.google.common.base.Preconditions;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.atlas.repository.graph.indexmanager.AtlasCardinalityMapper.toAtlasCardinality;
import static org.apache.atlas.repository.graph.indexmanager.PrimitiveClassMapper.getPrimitiveClass;
import static org.apache.atlas.repository.graphdb.AtlasCardinality.SINGLE;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.isReference;
import static org.apache.atlas.type.AtlasStructType.UNIQUE_ATTRIBUTE_SHADE_PROPERTY_PREFIX;
import static org.apache.atlas.type.AtlasTypeUtil.isArrayType;
import static org.apache.atlas.type.AtlasTypeUtil.isMapType;

@Component
public class AttributeIndexCreator {

    private static final Logger LOG = LoggerFactory.getLogger(AttributeIndexCreator.class);

    private final AtlasTypeRegistry typeRegistry;
    private final VertexIndexCreator vertexIndexCreator;
    private final EdgeIndexCreator edgeIndexCreator;

    @Inject
    public AttributeIndexCreator(AtlasTypeRegistry typeRegistry, VertexIndexCreator vertexIndexCreator, EdgeIndexCreator edgeIndexCreator) {
        this.typeRegistry = typeRegistry;
        this.vertexIndexCreator = vertexIndexCreator;
        this.edgeIndexCreator = edgeIndexCreator;
    }


    public void createIndexForAttribute(AtlasGraphManagement management, AtlasStructDef structDef, AtlasStructDef.AtlasAttributeDef attributeDef) {
        final String propertyName = AtlasStructType.AtlasAttribute.generateVertexPropertyName(attributeDef);
        AtlasCardinality cardinality = toAtlasCardinality(attributeDef.getCardinality());
        boolean isUnique = attributeDef.getIsUnique();
        boolean isIndexable = attributeDef.getIsIndexable();
        String attribTypeName = attributeDef.getTypeName();
        boolean isBuiltInType = AtlasTypeUtil.isBuiltInType(attribTypeName);
        boolean isArrayType = isArrayType(attribTypeName);
        boolean isArrayOfPrimitiveType = false;
        boolean isArrayOfEnum = false;
        AtlasType arrayElementType = null;
        boolean isMapType = isMapType(attribTypeName);
        final String uniqPropName = isUnique ? AtlasGraphUtilsV2.encodePropertyKey(UNIQUE_ATTRIBUTE_SHADE_PROPERTY_PREFIX + attributeDef.getName()) : null;
        final AtlasStructDef.AtlasAttributeDef.IndexType indexType = attributeDef.getIndexType();
        HashMap<String, Object> indexTypeESConfig = attributeDef.getIndexTypeESConfig();
        HashMap<String, HashMap<String, Object>> indexTypeESFields = attributeDef.getIndexTypeESFields();

        try {
            AtlasType atlasType = typeRegistry.getType(structDef.getName());
            AtlasType attributeType = typeRegistry.getType(attribTypeName);

            if (isClassificationType(attributeType)) {
                LOG.warn("Ignoring non-indexable attribute {}", attribTypeName);
            }

            if (isArrayType) {
                createLabelIfNeeded(management, propertyName, attribTypeName);

                AtlasArrayType arrayType = (AtlasArrayType) attributeType;
                boolean isReference = isReference(arrayType.getElementType());
                arrayElementType = arrayType.getElementType();
                isArrayOfPrimitiveType = arrayElementType.getTypeCategory().equals(TypeCategory.PRIMITIVE);
                isArrayOfEnum = arrayElementType.getTypeCategory().equals(TypeCategory.ENUM);

                if (!isReference && !isArrayOfPrimitiveType && !isArrayOfEnum) {
                    createPropertyKey(management, propertyName, ArrayList.class, SINGLE);
                }
            }

            if (isMapType) {
                createLabelIfNeeded(management, propertyName, attribTypeName);

                AtlasMapType mapType = (AtlasMapType) attributeType;
                boolean isReference = isReference(mapType.getValueType());

                if (!isReference) {
                    createPropertyKey(management, propertyName, HashMap.class, SINGLE);
                }
            }

            if (isEntityType(attributeType)) {
                createEdgeLabel(management, propertyName);

            } else if (isBuiltInType || isArrayOfPrimitiveType || isArrayOfEnum) {
                if (isRelationshipType(atlasType)) {
                    edgeIndexCreator.createEdgeIndex(management, propertyName, getPrimitiveClass(attribTypeName), cardinality, false);
                } else {
                    Class primitiveClassType;
                    boolean isStringField = false;

                    if (isArrayOfEnum) {
                        primitiveClassType = String.class;
                    } else {
                        primitiveClassType = isArrayOfPrimitiveType ? getPrimitiveClass(arrayElementType.getTypeName()) : getPrimitiveClass(attribTypeName);
                    }

                    if (primitiveClassType == String.class) {
                        isStringField = AtlasStructDef.AtlasAttributeDef.IndexType.STRING.equals(indexType);
                    }

                    vertexIndexCreator.createVertexIndex(
                            management,
                            propertyName,
                            UniqueKind.NONE,
                            primitiveClassType,
                            cardinality,
                            isIndexable,
                            false,
                            isStringField,
                            indexTypeESConfig,
                            indexTypeESFields
                    );

                    if (uniqPropName != null) {
                        vertexIndexCreator.createVertexIndex(
                                management,
                                uniqPropName,
                                UniqueKind.PER_TYPE_UNIQUE,
                                primitiveClassType,
                                cardinality,
                                isIndexable,
                                true,
                                isStringField,
                                new HashMap<>(),
                                new HashMap<>()
                        );
                    }

                }
            } else if (isEnumType(attributeType)) {
                if (isRelationshipType(atlasType)) {
                    edgeIndexCreator.createEdgeIndex(management, propertyName, String.class, cardinality, false);
                } else {
                    boolean isStringField = AtlasStructDef.AtlasAttributeDef.IndexType.STRING.equals(indexType);
                    vertexIndexCreator.createVertexIndex(
                            management,
                            propertyName,
                            UniqueKind.NONE,
                            String.class,
                            cardinality,
                            isIndexable,
                            false,
                            isStringField,
                            indexTypeESConfig,
                            indexTypeESFields
                    );

                    if (uniqPropName != null) {
                        vertexIndexCreator.createVertexIndex(
                                management,
                                uniqPropName,
                                UniqueKind.PER_TYPE_UNIQUE,
                                String.class,
                                cardinality,
                                isIndexable,
                                true,
                                false,
                                new HashMap<>(),
                                new HashMap<>()
                        );
                    }
                }
            } else if (isStructType(attributeType)) {
                AtlasStructDef attributeStructDef = typeRegistry.getStructDefByName(attribTypeName);
                updateIndexForTypeDef(management, attributeStructDef);
            }
        } catch (AtlasBaseException e) {
            LOG.error("No type exists for {}", attribTypeName, e);
        }
    }

    private void createLabelIfNeeded(final AtlasGraphManagement management, final String propertyName, final String attribTypeName) {
        // If any of the referenced typename is of type Entity or Struct then the edge label needs to be created
        for (String typeName : AtlasTypeUtil.getReferencedTypeNames(attribTypeName)) {
            if (typeRegistry.getEntityDefByName(typeName) != null || typeRegistry.getStructDefByName(typeName) != null) {
                // Create the edge label upfront to avoid running into concurrent call issue (ATLAS-2092)
                createEdgeLabel(management, propertyName);
            }
        }
    }

    private void createEdgeLabel(final AtlasGraphManagement management, final String propertyName) {
        // Create the edge label upfront to avoid running into concurrent call issue (ATLAS-2092)
        // ATLAS-2092 addresses this problem by creating the edge label upfront while type creation
        // which resolves the race condition during the entity creation

        String label = Constants.INTERNAL_PROPERTY_KEY_PREFIX + propertyName;

        createEdgeLabelUsingLabelName(management, label);
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

    private AtlasPropertyKey createPropertyKey(AtlasGraphManagement management, String propertyName, Class propertyClass, AtlasCardinality cardinality) {
        AtlasPropertyKey propertyKey = management.getPropertyKey(propertyName);

        if (propertyKey == null) {
            propertyKey = management.makePropertyKey(propertyName, propertyClass, cardinality);
        }

        return propertyKey;
    }

    public void updateIndexForTypeDef(AtlasGraphManagement management, AtlasBaseTypeDef typeDef) {
        Preconditions.checkNotNull(typeDef, "Cannot index on null typedefs");
        LOG.info("Index creation started for type {} complete", typeDef.getName());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating indexes for type name={}, definition={}", typeDef.getName(), typeDef.getClass());
        }
        addIndexForType(management, typeDef);
        LOG.info("Index creation for type {} complete", typeDef.getName());
    }

    private void addIndexForType(AtlasGraphManagement management, AtlasBaseTypeDef typeDef) {
        if (typeDef instanceof AtlasEnumDef) {
            // Only handle complex types like Struct, Classification and Entity
            return;
        }
        if (typeDef instanceof AtlasStructDef) {
            AtlasStructDef structDef = (AtlasStructDef) typeDef;
            List<AtlasStructDef.AtlasAttributeDef> attributeDefs = structDef.getAttributeDefs();
            if (CollectionUtils.isNotEmpty(attributeDefs)) {
                for (AtlasStructDef.AtlasAttributeDef attributeDef : attributeDefs) {
                    createIndexForAttribute(management, structDef, attributeDef);
                }
            }
        } else if (!AtlasTypeUtil.isBuiltInType(typeDef.getName())) {
            throw new IllegalArgumentException("bad data type" + typeDef.getName());
        }
    }

    private boolean isClassificationType(AtlasType type) {
        return type instanceof AtlasClassificationType;
    }

    private boolean isEnumType(AtlasType type) {
        return type instanceof AtlasEnumType;
    }

    private boolean isStructType(AtlasType type) {
        return type instanceof AtlasStructType;
    }

    private boolean isRelationshipType(AtlasType type) {
        return type instanceof AtlasRelationshipType;
    }

    private boolean isEntityType(AtlasType type) {
        return type instanceof AtlasEntityType;
    }
}
