package org.apache.atlas.repository.graph.indexmanager;

import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.type.AtlasStructType;

public class IndexManagerUtilFunctions {

    public static boolean isValidSearchWeight(int searchWeight) {
        if (searchWeight != -1) {
            return searchWeight >= 1 && searchWeight <= 10;
        }
        return true;
    }

    public static boolean isStringAttribute(AtlasStructType.AtlasAttribute attribute) {
        return AtlasBaseTypeDef.ATLAS_TYPE_STRING.equals(attribute.getTypeName());
    }
}
