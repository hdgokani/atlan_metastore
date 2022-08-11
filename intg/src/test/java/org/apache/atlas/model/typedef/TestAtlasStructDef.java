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

import org.apache.atlas.model.ModelTestUtil;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.type.AtlasType;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;

import static org.testng.Assert.*;


public class TestAtlasStructDef {

    @Test
    public void testStructDefSerDeEmpty() {
        AtlasStructDef structDef = new AtlasStructDef("emptyStructDef");

        String jsonString = AtlasType.toJson(structDef);

        AtlasStructDef structDef2 = AtlasType.fromJson(jsonString, AtlasStructDef.class);

        assertEquals(structDef2, structDef, "Incorrect serialization/deserialization of AtlasStructDef");
    }

    @Test
    public void testStructDefSerDe() {
        AtlasStructDef structDef = ModelTestUtil.getStructDef();

        String jsonString = AtlasType.toJson(structDef);

        AtlasStructDef structDef2 = AtlasType.fromJson(jsonString, AtlasStructDef.class);

        assertEquals(structDef2, structDef, "Incorrect serialization/deserialization of AtlasStructDef");
    }

    @Test
    public void testStructDefHasAttribute() {
        AtlasStructDef structDef = ModelTestUtil.getStructDef();

        for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
            assertTrue(structDef.hasAttribute(attributeDef.getName()));
        }

        assertFalse(structDef.hasAttribute("01234-xyzabc-;''-)("));
    }

    @Test
    public void testStructDefAddAttribute() {
        AtlasStructDef structDef = ModelTestUtil.newStructDef();

        structDef.addAttribute(new AtlasAttributeDef("newAttribute", AtlasBaseTypeDef.ATLAS_TYPE_INT));
        assertTrue(structDef.hasAttribute("newAttribute"));
    }

    @Test
    public void testStructDefRemoveAttribute() {
        AtlasStructDef structDef = ModelTestUtil.newStructDef();

        String attrName = structDef.getAttributeDefs().get(0).getName();
        assertTrue(structDef.hasAttribute(attrName));

        structDef.removeAttribute(attrName);
        assertFalse(structDef.hasAttribute(attrName));
    }

    @Test
    public void testStructDefSetAttributeDefs() {
        AtlasStructDef structDef = ModelTestUtil.newStructDef();

        List<AtlasAttributeDef> oldAttributes = structDef.getAttributeDefs();
        List<AtlasAttributeDef> newttributes = ModelTestUtil.newAttributeDefsWithAllBuiltInTypes("newAttributes");

        structDef.setAttributeDefs(newttributes);

        for (AtlasAttributeDef attributeDef : oldAttributes) {
            assertFalse(structDef.hasAttribute(attributeDef.getName()));
        }

        for (AtlasAttributeDef attributeDef : newttributes) {
            assertTrue(structDef.hasAttribute(attributeDef.getName()));
        }
    }


    @Test
    public void whenIndexTypeOfOneOfTheAttributesIsDifferentIndexCreationCheckShouldReturnTrue() {
        AtlasStructDef existingStruct = createStructWithDifferentIndexTypeAttribute(AtlasAttributeDef.IndexType.DEFAULT);
        AtlasStructDef modifiedStruct = createStructWithDifferentIndexTypeAttribute(AtlasAttributeDef.IndexType.STRING);

        boolean result = existingStruct.indexSettingsAreDifferentFrom(modifiedStruct);

        assertTrue(result);

    }

    private AtlasStructDef createStructWithDifferentIndexTypeAttribute(AtlasAttributeDef.IndexType indexType) {
        AtlasStructDef struct = new AtlasStructDef("struct");
        AtlasAttributeDef attributeDef = new AtlasAttributeDef("someAttribute", "struct", true, true);
        attributeDef.setIndexType(indexType);
        struct.addAttribute(attributeDef);

        return struct;
    }

    @Test
    public void whenIndexTypeConfigsAreDifferentIndexCreationCheckShouldReturnTrue() {
        AtlasStructDef existingStruct = createExistingStructForIndexConfig();
        AtlasStructDef modifiedStruct = createModifiedStructForIndexConfig();

        boolean result = existingStruct.indexSettingsAreDifferentFrom(modifiedStruct);

        assertTrue(result);

    }

    private AtlasStructDef createModifiedStructForIndexConfig() {
        AtlasStructDef modifiedStruct = new AtlasStructDef("struct");
        AtlasAttributeDef attributeDef = new AtlasAttributeDef("someAttribute", "struct", true, true);

        HashMap<String, Object> existingTypeConfig = new HashMap<>();
        existingTypeConfig.put("normalizer", "atlan_normalizer");
        existingTypeConfig.put("analyzer", "atlan_text_analyzer");

        attributeDef.setIndexTypeESConfig(existingTypeConfig);
        attributeDef.setIndexType(AtlasAttributeDef.IndexType.STRING);

        modifiedStruct.addAttribute(attributeDef);
        return modifiedStruct;
    }

    private AtlasStructDef createExistingStructForIndexConfig() {
        AtlasStructDef struct = new AtlasStructDef("struct");
        AtlasAttributeDef attributeDef = new AtlasAttributeDef("someAttribute", "struct", true, true);

        HashMap<String, Object> existingTypeConfig = new HashMap<>();
        existingTypeConfig.put("normalizer", "atlan_normalizer");

        attributeDef.setIndexTypeESConfig(existingTypeConfig);
        attributeDef.setIndexType(AtlasAttributeDef.IndexType.STRING);


        struct.addAttribute(attributeDef);
        return struct;
    }

    @Test
    public void whenOneOfTheConfigIsNullItShouldReturnTrue() {
        AtlasStructDef existingStruct = createExistingStructForIndexConfig();
        AtlasStructDef modifiedStruct = createModifiedStructForIndexConfig();
        existingStruct.getAttributeDefs().get(0).setIndexTypeESConfig(null);


        boolean result = existingStruct.indexSettingsAreDifferentFrom(modifiedStruct);

        assertTrue(result);
    }

    @Test
    public void whenBothConfigsAreNullItShouldReturnFalse() {
        AtlasStructDef existingStruct = createExistingStructForIndexConfig();
        AtlasStructDef modifiedStruct = createModifiedStructForIndexConfig();
        existingStruct.getAttributeDefs().get(0).setIndexTypeESConfig(null);
        modifiedStruct.getAttributeDefs().get(0).setIndexTypeESConfig(null);


        boolean result = existingStruct.indexSettingsAreDifferentFrom(modifiedStruct);

        assertFalse(result);

    }

    @Test
    public void whenIndexTypeFieldsHaveChangedIndexCreationCheckShouldReturnTrue() {
        AtlasStructDef existingStruct = createStructWithFields();
        AtlasStructDef modifiedStruct = createModifiedStructWithFields();

        boolean result = existingStruct.indexSettingsAreDifferentFrom(modifiedStruct);

        assertTrue(result);
    }

    private AtlasStructDef createModifiedStructWithFields() {
        AtlasStructDef modifiedStruct = createStructWithFields();

        HashMap<String, Object> fieldConfig = new HashMap<>();
        fieldConfig.put("type", "keyword");
        modifiedStruct.getAttributeDefs().get(0).getIndexTypeESFields().put("fieldName", fieldConfig);
        return modifiedStruct;
    }

    private AtlasStructDef createStructWithFields() {
        AtlasStructDef struct = new AtlasStructDef("struct");
        AtlasAttributeDef attributeDef = new AtlasAttributeDef("someAttribute", "struct", true, true);

        HashMap<String, HashMap<String, Object>> existingFields = new HashMap<>();
        HashMap<String, Object> fieldConfig = new HashMap<>();
        fieldConfig.put("type", "text");
        fieldConfig.put("analyzer", "atlan_text_analyzer");
        existingFields.put("fieldName", fieldConfig);

        HashMap<String, Object> existingTypeConfig = new HashMap<>();
        existingTypeConfig.put("normalizer", "atlan_normalizer");

        attributeDef.setIndexTypeESConfig(existingTypeConfig);
        attributeDef.setIndexType(AtlasAttributeDef.IndexType.STRING);
        attributeDef.setIndexTypeESFields(existingFields);

        struct.addAttribute(attributeDef);
        return struct;
    }

    @Test
    public void whenBothFieldsAreNullItShouldReturnTrue() {
        AtlasStructDef existingStruct = createExistingStructForIndexConfig();
        AtlasStructDef modifiedStruct = createExistingStructForIndexConfig();

        existingStruct.getAttributeDefs().get(0).setIndexTypeESFields(null);
        modifiedStruct.getAttributeDefs().get(0).setIndexTypeESFields(null);

        boolean result = existingStruct.indexSettingsAreDifferentFrom(modifiedStruct);

        assertFalse(result);
    }

    @Test
    public void whenEverythingIsSameItShouldReturnFalse() {
        AtlasStructDef existingStruct = createExistingStructForIndexConfig();
        AtlasStructDef modifiedStruct = createExistingStructForIndexConfig();

        boolean result = existingStruct.indexSettingsAreDifferentFrom(modifiedStruct);

        assertFalse(result);
    }

}
