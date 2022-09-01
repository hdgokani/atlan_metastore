package org.apache.atlas.accesscontrol;

import org.apache.atlas.accesscontrol.persona.AtlasPersonaService;
import org.apache.atlas.accesscontrol.purpose.AtlasPurposeService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.accesscontrol.AccessControlUtil.getPolicyCategory;
import static org.apache.atlas.repository.Constants.PERSONA_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.POLICY_CATEGORY_PERSONA;
import static org.apache.atlas.repository.Constants.POLICY_CATEGORY_PURPOSE;
import static org.apache.atlas.repository.Constants.POLICY_ENTITY_TYPE;

@Component
public class AtlasAccessControlService {

    private final AtlasPersonaService personaService;
    private final AtlasPurposeService purposeService;
    private final EntityGraphRetriever entityRetriever;

    @Inject
    public AtlasAccessControlService(AtlasPersonaService personaService, AtlasPurposeService purposeService, EntityGraphRetriever entityRetriever) {
        this.personaService = personaService;
        this.purposeService = purposeService;
        this.entityRetriever = entityRetriever;
    }


    public EntityMutationResponse createOrUpdate(AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasBaseException {
        EntityMutationResponse ret;

        if(entityWithExtInfo.getEntity().getTypeName().equals(PERSONA_ENTITY_TYPE)) {
            ret = personaService.createOrUpdatePersona(entityWithExtInfo);
        } else {
            ret = purposeService.createOrUpdatePurpose(entityWithExtInfo);
        }

        return ret;
    }

    public void delete(String guid) throws AtlasBaseException {
        AtlasEntityWithExtInfo accessControlEntity = entityRetriever.toAtlasEntityWithExtInfo(guid);

        if(accessControlEntity.getEntity().getTypeName().equals(PERSONA_ENTITY_TYPE)) {
            personaService.deletePersona(accessControlEntity);
        } else {
            purposeService.deletePurpose(accessControlEntity);
        }
    }

    public void deletePolicy(String guid) throws AtlasBaseException {
        AtlasEntity policy = entityRetriever.toAtlasEntity(guid);

        if(!POLICY_ENTITY_TYPE.equals(policy.getTypeName())) {
            throw new AtlasBaseException(BAD_REQUEST, "Please provide entity of type " + POLICY_ENTITY_TYPE);
        }

        String policyCategory = getPolicyCategory(policy);
        switch (policyCategory) {
            case POLICY_CATEGORY_PERSONA:
                personaService.deletePersonaPolicy(policy);
                break;

            case POLICY_CATEGORY_PURPOSE:
                purposeService.deletePurposePolicy(policy);
                break;

            default:
                throw new AtlasBaseException(BAD_REQUEST, "Please provide valid accessControlPolicyCategory");
        }
    }

    public EntityMutationResponse createOrUpdatePolicy(AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasBaseException {
        EntityMutationResponse ret;
        String policyCategory = getPolicyCategory(entityWithExtInfo.getEntity());

        switch (policyCategory) {
            case POLICY_CATEGORY_PERSONA:
                ret = personaService.createOrUpdatePersonaPolicy(entityWithExtInfo);
                break;

            case POLICY_CATEGORY_PURPOSE:
                ret = purposeService.createOrUpdatePurposePolicy(entityWithExtInfo);
                break;

            default:
                throw new AtlasBaseException(BAD_REQUEST, "Please provide valid accessControlPolicyCategory");
        }

        return ret;
    }
}
