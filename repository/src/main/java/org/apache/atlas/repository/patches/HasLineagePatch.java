package org.apache.atlas.repository.patches;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.pc.WorkItemManager;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import static org.apache.atlas.AtlasClient.CATALOG_SUPER_TYPE;
import static org.apache.atlas.AtlasClient.PROCESS_SUPER_TYPE;
import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.APPLIED;
import static org.apache.atlas.repository.Constants.ENTITY_TYPE_PROPERTY_KEY;
import static org.apache.atlas.repository.graph.GraphHelper.getTypeName;
import static org.apache.atlas.type.Constants.HAS_LINEAGE;

public class HasLineagePatch extends AtlasPatchHandler {
    private static final Logger LOG = LoggerFactory.getLogger(HasLineagePatch.class);

    private static final String PATCH_ID = "JAVA_PATCH_0000_011";
    private static final String PATCH_DESCRIPTION = "Set __hasLineage attribute at vertex if missing.";

    private final PatchContext context;

    public HasLineagePatch(PatchContext context) {
        super(context.getPatchRegistry(), PATCH_ID, PATCH_DESCRIPTION);
        this.context = context;
    }

    @Override
    public void apply() throws AtlasBaseException {
        if (!AtlasConfiguration.HAS_LINEAGE_UPDATE_PATCH.getBoolean()) {
            LOG.info("HasLineagePatch: Skipped, since not enabled!");
            return;
        }

        HasLineagePatchProcessor patchProcessor = new HasLineagePatch.HasLineagePatchProcessor(context);

        patchProcessor.apply();

        setStatus(APPLIED);

        LOG.info("HasLineagePatch.apply(): patchId={}, status={}", getPatchId(), getStatus());
    }

    public static class HasLineagePatchProcessor extends ConcurrentPatchProcessor {

        public HasLineagePatchProcessor(PatchContext context) {
            super(context);
        }

        @Override
        protected void prepareForExecution() throws AtlasBaseException {
            //do nothing

        }

        @Override
        protected void submitVerticesToUpdate(WorkItemManager manager) {
            AtlasTypeRegistry typeRegistry = getTypeRegistry();
            AtlasGraph graph = getGraph();

            for (AtlasEntityType entityType : typeRegistry.getAllEntityTypes()) {
                boolean isProcess = entityType.getTypeAndAllSuperTypes().contains(PROCESS_SUPER_TYPE);
                boolean isCatalog = entityType.getTypeAndAllSuperTypes().contains(CATALOG_SUPER_TYPE);

                if (isProcess || isCatalog) {

                    LOG.info("finding entities of type {}", entityType.getTypeName());

                    Iterable<Object> iterable = graph.query().has(Constants.ENTITY_TYPE_PROPERTY_KEY, entityType.getTypeName()).vertexIds();
                    int count = 0;

                    for (Iterator<Object> iter = iterable.iterator(); iter.hasNext(); ) {
                        Object vertexId = iter.next();

                        manager.checkProduce((Long) vertexId);

                        count++;
                    }
                    LOG.info("found {} entities of type {}", count, entityType.getTypeName());
                }
            }
        }

        @Override
        protected void processVertexItem(Long vertexId, AtlasVertex vertex, String typeName, AtlasEntityType entityType) throws AtlasBaseException {

            Boolean hasLineageProperty = vertex.getPropertyKeys().contains(HAS_LINEAGE);

            if (hasLineageProperty) {
                LOG.info("skipping since property already present");
                return;
            }

            AtlasGraphUtilsV2.setEncodedProperty(vertex, HAS_LINEAGE, false);
        }
    }
}
