package org.apache.atlas.repository.graph.indexmanager;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.apache.atlas.ApplicationProperties.INDEX_BACKEND_CONF;

public class IndexApplicabilityChecker {

    private static final Logger LOG = LoggerFactory.getLogger(IndexApplicabilityChecker.class);


    private static final List<Class> INDEX_EXCLUSION_CLASSES = new ArrayList<Class>() {
        {
            add(BigDecimal.class);
            add(BigInteger.class);
        }
    };

    public static boolean isIndexApplicable(Class propertyClass, AtlasCardinality cardinality) {
        String indexBackend = "";
        try {
            indexBackend = ApplicationProperties.get().getString(INDEX_BACKEND_CONF);
        } catch (AtlasException e) {
            LOG.error("Failed to read property {}", INDEX_BACKEND_CONF);
            e.printStackTrace();
        }

        if (StringUtils.isNotEmpty(indexBackend) && "elasticsearch".equals(indexBackend)) {
            return !INDEX_EXCLUSION_CLASSES.contains(propertyClass);
        }

        return !(INDEX_EXCLUSION_CLASSES.contains(propertyClass) || cardinality.isMany());
    }
}
