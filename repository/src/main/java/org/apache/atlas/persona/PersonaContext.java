/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.persona;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;

import java.util.ArrayList;
import java.util.List;

import static org.apache.atlas.persona.AtlasPersonaUtil.getDataPolicyMaskType;
import static org.apache.atlas.persona.AtlasPersonaUtil.getIsAllow;
import static org.apache.atlas.repository.Constants.PERSONA_DATA_POLICY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.PERSONA_GLOSSARY_POLICY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.PERSONA_METADATA_POLICY_ENTITY_TYPE;

public class PersonaContext {


    private AtlasEntityWithExtInfo personaExtInfo;
    private AtlasEntity personaPolicy;
    private AtlasEntity existingPersonaPolicy;
    private AtlasEntity connection;
    private boolean isCreateNewPersona;
    private boolean isCreateNewPersonaPolicy;
    private boolean isUpdatePersonaPolicy;
    private boolean isDeletePersonaPolicy;

    private boolean isAllowPolicy;
    private boolean updateIsAllow;

    private boolean isMetadataPolicy;
    private boolean isGlossaryPolicy;
    private boolean isDataPolicy;
    private boolean isDataMaskPolicy;

    private List<RangerPolicy> excessExistingRangerPolicies = new ArrayList<>();

    public PersonaContext() {}

    public PersonaContext(AtlasEntityWithExtInfo personaExtInfo) {
        this.personaExtInfo = personaExtInfo;
    }

    public PersonaContext(AtlasEntityWithExtInfo personaExtInfo, AtlasEntity personaPolicy) {
        this.personaExtInfo = personaExtInfo;
        this.personaPolicy = personaPolicy;
    }

    public AtlasEntityWithExtInfo getPersonaExtInfo() {
        return personaExtInfo;
    }

    public AtlasEntity getPersona() {
        return personaExtInfo.getEntity();
    }

    public void setPersonaExtInfo(AtlasEntityWithExtInfo personaExtInfo) {
        this.personaExtInfo = personaExtInfo;
    }

    public AtlasEntity getPersonaPolicy() {
        return personaPolicy;
    }

    public void setPersonaPolicy(AtlasEntity personaPolicy) {
        this.personaPolicy = personaPolicy;
    }

    public boolean isCreateNewPersona() {
        return isCreateNewPersona;
    }

    public void setCreateNewPersona(boolean createNewPersona) {
        isCreateNewPersona = createNewPersona;
    }

    public boolean isCreateNewPersonaPolicy() {
        return isCreateNewPersonaPolicy;
    }

    public void setCreateNewPersonaPolicy(boolean createNewPersonaPolicy) {
        isCreateNewPersonaPolicy = createNewPersonaPolicy;
    }

    public boolean isUpdatePersonaPolicy() {
        return isUpdatePersonaPolicy;
    }

    public void setUpdatePersonaPolicy(boolean updatePersonaPolicy) {
        isUpdatePersonaPolicy = updatePersonaPolicy;
    }

    public boolean isDeletePersonaPolicy() {
        return isDeletePersonaPolicy;
    }

    public void setDeletePersonaPolicy(boolean deletePersonaPolicy) {
        isDeletePersonaPolicy = deletePersonaPolicy;
    }

    public AtlasEntity getExistingPersonaPolicy() {
        return existingPersonaPolicy;
    }

    public void setExistingPersonaPolicy(AtlasEntity existingPersonaPolicy) {
        this.existingPersonaPolicy = existingPersonaPolicy;
    }

    public List<RangerPolicy> getExcessExistingRangerPolicies() {
        return excessExistingRangerPolicies;
    }

    public void setExcessExistingRangerPolicies(List<RangerPolicy> excessExistingRangerPolicies) {
        this.excessExistingRangerPolicies = excessExistingRangerPolicies;
    }

    public void addExcessExistingRangerPolicy(RangerPolicy excessExistingRangerPolicy) {
        this.excessExistingRangerPolicies.add(excessExistingRangerPolicy);
    }

    public boolean isAllowPolicy() {
        return isAllowPolicy;
    }

    public void setAllowPolicy(boolean allowPolicy) {
        isAllowPolicy = allowPolicy;
    }

    public boolean isUpdateIsAllow() {
        return updateIsAllow;
    }

    public void setAllowPolicyUpdate() {
        if (existingPersonaPolicy != null) {
            updateIsAllow = getIsAllow(existingPersonaPolicy) != isAllowPolicy;
        }
    }

    public boolean isMetadataPolicy() {
        return isMetadataPolicy;
    }

    public boolean isGlossaryPolicy() {
        return isGlossaryPolicy;
    }

    public boolean isDataPolicy() {
        return isDataPolicy;
    }

    public boolean isDataMaskPolicy() {
        return isDataMaskPolicy;
    }

    public void setPolicyType(){
        if (personaPolicy != null) {
            String type = personaPolicy.getTypeName();

            switch (type) {
                case PERSONA_METADATA_POLICY_ENTITY_TYPE:
                    isMetadataPolicy = true;
                    break;
                case PERSONA_GLOSSARY_POLICY_ENTITY_TYPE:
                    isGlossaryPolicy = true;
                    break;
                case PERSONA_DATA_POLICY_ENTITY_TYPE:
                    isDataPolicy = true;
                    setDataPolicyType();
            }
        }
    }

    private void setDataPolicyType() {
        if (StringUtils.isNotEmpty(getDataPolicyMaskType(personaPolicy))) {//TODO: review this condition
            isDataMaskPolicy = true;
        }
    }

    public AtlasEntity getConnection() {
        return connection;
    }

    public void setConnection(AtlasEntity connection) {
        this.connection = connection;
    }
}
