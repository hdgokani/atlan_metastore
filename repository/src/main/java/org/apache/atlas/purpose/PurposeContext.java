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
package org.apache.atlas.purpose;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;

import java.util.ArrayList;
import java.util.List;

import static org.apache.atlas.PersonaPurposeCommonUtil.getDataPolicyMaskType;
import static org.apache.atlas.purpose.AtlasPurposeUtil.getIsAllow;
import static org.apache.atlas.repository.Constants.PERSONA_DATA_POLICY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.PURPOSE_DATA_POLICY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.PURPOSE_METADATA_POLICY_ENTITY_TYPE;

public class PurposeContext {


    private AtlasEntityWithExtInfo purposeExtInfo;
    private AtlasEntity purposePolicy;
    private AtlasEntity existingPurposePolicy;
    private boolean isCreateNewPurpose;
    private boolean isCreateNewPurposePolicy;
    private boolean isUpdatePurposePolicy;
    private boolean isDeletePurposePolicy;


    private boolean isAllowPolicy;
    private boolean updateIsAllow = false;

    private List<RangerPolicy> excessExistingRangerPolicies = new ArrayList<>();

    private boolean isMetadataPolicy = false;
    private boolean isDataPolicy = false;
    private boolean isDataMaskPolicy;


    public PurposeContext() {}

    public PurposeContext(AtlasEntityWithExtInfo purposeExtInfo) {
        this.purposeExtInfo = purposeExtInfo;
    }

    public PurposeContext(AtlasEntityWithExtInfo purposeExtInfo, AtlasEntity purposePolicy) {
        this.purposeExtInfo = purposeExtInfo;
        this.purposePolicy = purposePolicy;
    }

    public AtlasEntityWithExtInfo getPurposeExtInfo() {
        return purposeExtInfo;
    }

    public AtlasEntity getPurpose() {
        return purposeExtInfo.getEntity();
    }

    public void setPurposeExtInfo(AtlasEntityWithExtInfo purposeExtInfo) {
        this.purposeExtInfo = purposeExtInfo;
    }

    public AtlasEntity getPurposePolicy() {
        return purposePolicy;
    }

    public void setPurposePolicy(AtlasEntity purposePolicy) {
        this.purposePolicy = purposePolicy;
        setPolicyType();
    }

    public boolean isCreateNewPurpose() {
        return isCreateNewPurpose;
    }

    public void setCreateNewPurpose(boolean createNewPurpose) {
        isCreateNewPurpose = createNewPurpose;
    }

    public boolean isCreateNewPurposePolicy() {
        return isCreateNewPurposePolicy;
    }

    public void setCreateNewPurposePolicy(boolean createNewPurposePolicy) {
        isCreateNewPurposePolicy = createNewPurposePolicy;
    }

    public boolean isUpdatePurposePolicy() {
        return isUpdatePurposePolicy;
    }

    public void setUpdatePurposePolicy(boolean updatePurposePolicy) {
        isUpdatePurposePolicy = updatePurposePolicy;
    }

    public boolean isDeletePurposePolicy() {
        return isDeletePurposePolicy;
    }

    public void setDeletePurposePolicy(boolean deletePurposePolicy) {
        isDeletePurposePolicy = deletePurposePolicy;
    }

    public AtlasEntity getExistingPurposePolicy() {
        return existingPurposePolicy;
    }

    public void setExistingPurposePolicy(AtlasEntity existingPurposePolicy) {
        this.existingPurposePolicy = existingPurposePolicy;
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
        if (existingPurposePolicy != null) {
            updateIsAllow = getIsAllow(existingPurposePolicy) != isAllowPolicy;
        }
    }

    public boolean isDataPolicy() {
        return isDataPolicy;
    }

    public boolean isDataMaskPolicy() {
        return isDataMaskPolicy;
    }

    public boolean isMetadataPolicy() {
        return isMetadataPolicy;
    }

    public void setMetadataPolicy(boolean metadataPolicy) {
        isMetadataPolicy = metadataPolicy;
    }

    public void setPolicyType() {
        if (getPurposePolicy() != null) {
            String type = getPurposePolicy().getTypeName();

            switch (type) {
                case PURPOSE_METADATA_POLICY_ENTITY_TYPE:
                    isMetadataPolicy = true;
                    break;
                case PURPOSE_DATA_POLICY_ENTITY_TYPE:
                    isDataPolicy = true;
                    setDataPolicyType();
            }
        }
    }

    private void setDataPolicyType() {
        if (StringUtils.isNotEmpty(getDataPolicyMaskType(purposePolicy))) {//TODO: review this condition
            isDataMaskPolicy = true;
        }
    }
}
