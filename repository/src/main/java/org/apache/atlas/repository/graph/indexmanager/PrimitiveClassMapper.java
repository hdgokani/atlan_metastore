package org.apache.atlas.repository.graph.indexmanager;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.*;

public class PrimitiveClassMapper {

    public static Class getPrimitiveClass(String attribTypeName) {
        String attributeTypeName = attribTypeName.toLowerCase();

        switch (attributeTypeName) {
            case ATLAS_TYPE_BOOLEAN:
                return Boolean.class;
            case ATLAS_TYPE_BYTE:
                return Byte.class;
            case ATLAS_TYPE_SHORT:
                return Short.class;
            case ATLAS_TYPE_INT:
                return Integer.class;
            case ATLAS_TYPE_LONG:
            case ATLAS_TYPE_DATE:
                return Long.class;
            case ATLAS_TYPE_FLOAT:
                return Float.class;
            case ATLAS_TYPE_DOUBLE:
                return Double.class;
            case ATLAS_TYPE_BIGINTEGER:
                return BigInteger.class;
            case ATLAS_TYPE_BIGDECIMAL:
                return BigDecimal.class;
            case ATLAS_TYPE_STRING:
                return String.class;
        }

        throw new IllegalArgumentException(String.format("Unknown primitive typename %s", attribTypeName));
    }
}
