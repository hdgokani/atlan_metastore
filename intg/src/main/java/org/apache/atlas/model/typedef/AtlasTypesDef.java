/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.model.typedef;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.commons.collections.CollectionUtils;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.*;
import java.util.stream.Collectors;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasTypesDef {
    private List<AtlasEnumDef> enumDefs;
    private List<AtlasStructDef> structDefs;
    private List<AtlasClassificationDef> classificationDefs;
    private List<AtlasEntityDef> entityDefs;
    private List<AtlasRelationshipDef> relationshipDefs;
    private List<AtlasBusinessMetadataDef> businessMetadataDefs;

    public AtlasTypesDef() {
        enumDefs = new ArrayList<>();
        structDefs = new ArrayList<>();
        classificationDefs = new ArrayList<>();
        entityDefs = new ArrayList<>();
        relationshipDefs = new ArrayList<>();
        businessMetadataDefs = new ArrayList<>();
    }

    public AtlasTypesDef(AtlasTypesDef atlasTypesDef) {
        this.enumDefs = atlasTypesDef.enumDefs;
        this.structDefs = atlasTypesDef.structDefs;
        this.classificationDefs = atlasTypesDef.classificationDefs;
        this.entityDefs = atlasTypesDef.entityDefs;
        this.relationshipDefs = atlasTypesDef.relationshipDefs;
        this.businessMetadataDefs = atlasTypesDef.businessMetadataDefs;
    }

    /**
     * tolerate typeDef creations that do not contain relationshipDefs, so that
     * the older calls will still work.
     *
     * @param enumDefs
     * @param structDefs
     * @param classificationDefs
     * @param entityDefs
     */
    public AtlasTypesDef(List<AtlasEnumDef> enumDefs, List<AtlasStructDef> structDefs,
                         List<AtlasClassificationDef> classificationDefs, List<AtlasEntityDef> entityDefs) {
        this(enumDefs, structDefs, classificationDefs, entityDefs, new ArrayList<>(), new ArrayList<>());
    }

    /**
     * Create the TypesDef. This created definitions for each of the types.
     *
     * @param enumDefs
     * @param structDefs
     * @param classificationDefs
     * @param entityDefs
     * @param relationshipDefs
     */
    public AtlasTypesDef(List<AtlasEnumDef> enumDefs,
                         List<AtlasStructDef> structDefs,
                         List<AtlasClassificationDef> classificationDefs,
                         List<AtlasEntityDef> entityDefs,
                         List<AtlasRelationshipDef> relationshipDefs) {
        this(enumDefs, structDefs, classificationDefs, entityDefs, relationshipDefs, new ArrayList<>());
    }

    public AtlasTypesDef(List<AtlasEnumDef> enumDefs,
                         List<AtlasStructDef> structDefs,
                         List<AtlasClassificationDef> classificationDefs,
                         List<AtlasEntityDef> entityDefs,
                         List<AtlasRelationshipDef> relationshipDefs,
                         List<AtlasBusinessMetadataDef> businessMetadataDefs) {
        this.enumDefs = enumDefs;
        this.structDefs = structDefs;
        this.classificationDefs = classificationDefs;
        this.entityDefs = entityDefs;
        this.relationshipDefs = relationshipDefs;
        this.businessMetadataDefs = businessMetadataDefs;
    }

    public List<AtlasEnumDef> getEnumDefs() {
        return enumDefs;
    }

    public void setEnumDefs(List<AtlasEnumDef> enumDefs) {
        this.enumDefs = enumDefs;
    }

    public List<AtlasStructDef> getStructDefs() {
        return structDefs;
    }

    public void setStructDefs(List<AtlasStructDef> structDefs) {
        this.structDefs = structDefs;
    }

    public List<AtlasClassificationDef> getClassificationDefs() {
        return classificationDefs;
    }

    public List<AtlasEntityDef> getEntityDefs() {
        return entityDefs;
    }

    public void setEntityDefs(List<AtlasEntityDef> entityDefs) {
        this.entityDefs = entityDefs;
    }

    public void setClassificationDefs(List<AtlasClassificationDef> classificationDefs) {
        this.classificationDefs = classificationDefs;
    }

    public List<AtlasRelationshipDef> getRelationshipDefs() {
        return relationshipDefs;
    }

    public void setRelationshipDefs(List<AtlasRelationshipDef> relationshipDefs) {
        this.relationshipDefs = relationshipDefs;
    }

    public void setBusinessMetadataDefs(List<AtlasBusinessMetadataDef> businessMetadataDefs) {
        this.businessMetadataDefs = businessMetadataDefs;
    }

    public List<AtlasBusinessMetadataDef> getBusinessMetadataDefs() {
        return businessMetadataDefs;
    }

    public boolean hasClassificationDef(String name) {
        return hasTypeDef(classificationDefs, name);
    }

    public boolean hasEnumDef(String name) {
        return hasTypeDef(enumDefs, name);
    }

    public boolean hasStructDef(String name) {
        return hasTypeDef(structDefs, name);
    }

    public boolean hasEntityDef(String name) {
        return hasTypeDef(entityDefs, name);
    }

    public boolean hasRelationshipDef(String name) {
        return hasTypeDef(relationshipDefs, name);
    }

    public boolean hasBusinessMetadataDef(String name) {
        return hasTypeDef(businessMetadataDefs, name);
    }

    private <T extends AtlasBaseTypeDef> boolean hasTypeDef(Collection<T> typeDefs, String name) {
        if (CollectionUtils.isNotEmpty(typeDefs)) {
            for (T typeDef : typeDefs) {
                if (typeDef.getName().equals(name)) {
                    return true;
                }
            }
        }

        return false;
    }

    @JsonIgnore
    public boolean isEmpty() {
        return CollectionUtils.isEmpty(enumDefs) &&
                CollectionUtils.isEmpty(structDefs) &&
                CollectionUtils.isEmpty(classificationDefs) &&
                CollectionUtils.isEmpty(entityDefs) &&
                CollectionUtils.isEmpty(relationshipDefs) &&
                CollectionUtils.isEmpty(businessMetadataDefs);
    }

    public void clear() {
        if (enumDefs != null) {
            enumDefs.clear();
        }

        if (structDefs != null) {
            structDefs.clear();
        }

        if (classificationDefs != null) {
            classificationDefs.clear();
        }

        if (entityDefs != null) {
            entityDefs.clear();
        }
        if (relationshipDefs != null) {
            relationshipDefs.clear();
        }

        if (businessMetadataDefs != null) {
            businessMetadataDefs.clear();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("AtlasTypesDef{");
        sb.append("enumDefs={");
        AtlasBaseTypeDef.dumpObjects(enumDefs, sb);
        sb.append("}");
        sb.append("structDefs={");
        AtlasBaseTypeDef.dumpObjects(structDefs, sb);
        sb.append("}");
        sb.append("classificationDefs={");
        AtlasBaseTypeDef.dumpObjects(classificationDefs, sb);
        sb.append("}");
        sb.append("entityDefs={");
        AtlasBaseTypeDef.dumpObjects(entityDefs, sb);
        sb.append("}");
        sb.append("relationshipDefs={");
        AtlasBaseTypeDef.dumpObjects(relationshipDefs, sb);
        sb.append("businessMetadataDefs={");
        AtlasBaseTypeDef.dumpObjects(businessMetadataDefs, sb);
        sb.append("}");

        return sb.toString();
    }

    public boolean haveIndexSettingsChanged(AtlasTypesDef newTypeDefinitions) {
        Map<String, AtlasStructDef> existingTypeNameMap = newTypeDefinitions.createNameMapFromStructTypeDefs();
        Map<String, AtlasStructDef> newTypeNameMap = createNameMapFromStructTypeDefs();
        return existingTypeNameMap.entrySet()
                .stream()
                .filter(entry -> newTypeNameMap.containsKey(entry.getKey()))
                .anyMatch(entry -> entry.getValue().indexSettingsAreDifferentFrom(newTypeNameMap.get(entry.getKey())));
    }

    public Map<String, AtlasStructDef> createNameMapFromStructTypeDefs() {
        Map<String, AtlasStructDef> nameMap = new HashMap<>();

        addTypeToMap(getEntityDefs(), nameMap);
        addTypeToMap(getClassificationDefs(), nameMap);
        addTypeToMap(getBusinessMetadataDefs(), nameMap);
        addTypeToMap(getRelationshipDefs(), nameMap);
        addTypeToMap(getBusinessMetadataDefs(), nameMap);

        return nameMap;
    }

    private void addTypeToMap(List<? extends AtlasStructDef> typeDefinitions, Map<String, AtlasStructDef> nameMap) {
        for (AtlasStructDef typeDef : typeDefinitions) {
            nameMap.put(typeDef.getName(), typeDef);
        }
    }

    public AtlasTypesDef getUpdatedTypesDef(AtlasTypesDef existingTypesDef) {
        AtlasTypesDef updatedTypesDef = new AtlasTypesDef(this);

        updatedTypesDef.filterUpdatedTypes(existingTypesDef);

        return updatedTypesDef;
    }

    private void filterUpdatedTypes(AtlasTypesDef existingTypesDef) {
        entityDefs = entityDefs.stream()
                .filter(type -> existingTypesDef.entityDefs.stream().anyMatch(existingType -> existingType.getName().equals(type.getName())))
                .collect(Collectors.toList());

        enumDefs = enumDefs.stream()
                .filter(type -> existingTypesDef.enumDefs.stream().anyMatch(existingType -> existingType.getName().equals(type.getName())))
                .collect(Collectors.toList());

        structDefs = structDefs.stream()
                .filter(type -> existingTypesDef.structDefs.stream().anyMatch(existingType -> existingType.getName().equals(type.getName())))
                .collect(Collectors.toList());

        businessMetadataDefs = businessMetadataDefs.stream()
                .filter(type -> existingTypesDef.businessMetadataDefs.stream().anyMatch(existingType -> existingType.getName().equals(type.getName())))
                .collect(Collectors.toList());

        classificationDefs = classificationDefs.stream()
                .filter(type -> existingTypesDef.classificationDefs.stream().anyMatch(existingType -> existingType.getDisplayName().equals(type.getDisplayName())))
                .collect(Collectors.toList());

        setGuidForExistingClassifications(existingTypesDef);

        relationshipDefs = relationshipDefs.stream()
                .filter(type -> existingTypesDef.relationshipDefs.stream().anyMatch(existingType -> existingType.getName().equals(type.getName())))
                .collect(Collectors.toList());
    }

    private void setGuidForExistingClassifications(AtlasTypesDef existingTypesDef) {
        classificationDefs
                .forEach(classification -> {
                            AtlasClassificationDef classificationDef = existingTypesDef.classificationDefs
                                    .stream()
                                    .filter(existingClassification -> existingClassification.getDisplayName().equals(classification.getDisplayName()))
                                    .findFirst()
                                    .orElseThrow(() -> new IllegalArgumentException());
                            classification.setFieldsForExistingClassification(classificationDef);

                        }
                );
    }

    public AtlasTypesDef getCreatedOrDeletedTypesDef(AtlasTypesDef atlasTypesDef) {
        AtlasTypesDef createdOrDeletedTypes = new AtlasTypesDef(this);

        createdOrDeletedTypes.filterCreatedOrDeletedTypes(atlasTypesDef);

        return createdOrDeletedTypes;

    }

    public void filterCreatedOrDeletedTypes(AtlasTypesDef atlasTypesDef) {

        entityDefs = entityDefs.stream()
                .filter(type -> atlasTypesDef.entityDefs.stream().noneMatch(existingType -> existingType.getName().equals(type.getName())))
                .collect(Collectors.toList());

        enumDefs = enumDefs.stream()
                .filter(type -> atlasTypesDef.enumDefs.stream().noneMatch(existingType -> existingType.getName().equals(type.getName())))
                .collect(Collectors.toList());

        structDefs = structDefs.stream()
                .filter(type -> atlasTypesDef.structDefs.stream().noneMatch(existingType -> existingType.getName().equals(type.getName())))
                .collect(Collectors.toList());

        businessMetadataDefs = businessMetadataDefs.stream()
                .filter(type -> atlasTypesDef.businessMetadataDefs.stream().noneMatch(existingType -> existingType.getName().equals(type.getName())))
                .collect(Collectors.toList());

        classificationDefs = classificationDefs.stream()
                .filter(type -> atlasTypesDef.classificationDefs.stream().noneMatch(existingType -> existingType.getDisplayName().equals(type.getDisplayName())))
                .collect(Collectors.toList());

        relationshipDefs = relationshipDefs.stream()
                .filter(type -> atlasTypesDef.relationshipDefs.stream().noneMatch(existingType -> existingType.getName().equals(type.getName())))
                .collect(Collectors.toList());


    }
}
