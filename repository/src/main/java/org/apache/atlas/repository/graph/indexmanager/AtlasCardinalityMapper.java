package org.apache.atlas.repository.graph.indexmanager;

import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.graphdb.AtlasCardinality;

import static org.apache.atlas.repository.graphdb.AtlasCardinality.*;

public class AtlasCardinalityMapper {

    public static AtlasCardinality toAtlasCardinality(AtlasStructDef.AtlasAttributeDef.Cardinality cardinality) {
        switch (cardinality) {
            case SINGLE:
                return SINGLE;
            case LIST:
                return LIST;
            case SET:
                return SET;
        }
        // Should never reach this point
        throw new IllegalArgumentException(String.format("Bad cardinality %s", cardinality));
    }
}
