package org.apache.atlas.repository.graph.indexmanager;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.listener.ChangedTypeDefs;
import org.apache.atlas.repository.IndexException;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class GraphTransactionManager {

    private static final Logger LOG = LoggerFactory.getLogger(GraphTransactionManager.class);
    protected static boolean recomputeIndexedKeys = true;

    public void commit(AtlasGraphManagement management) throws IndexException {
        try {
            management.commit();

            recomputeIndexedKeys = true;
        } catch (Exception e) {
            LOG.error("Index commit failed", e);
            throw new IndexException("Index commit failed ", e);
        }
    }

    public void rollback(AtlasGraphManagement management) throws IndexException {
        try {
            management.rollback();

            recomputeIndexedKeys = true;
        } catch (Exception e) {
            LOG.error("Index rollback failed ", e);
            throw new IndexException("Index rollback failed ", e);
        }
    }

    protected void attemptRollback(ChangedTypeDefs changedTypeDefs, AtlasGraphManagement management)
            throws AtlasBaseException {
        if (null != management) {
            try {
                rollback(management);
            } catch (IndexException e) {
                LOG.error("Index rollback has failed", e);
                throw new AtlasBaseException(AtlasErrorCode.INDEX_ROLLBACK_FAILED, e,
                        changedTypeDefs.toString());
            }
        }
    }
}
