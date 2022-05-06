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
package org.apache.atlas.authorize;


import org.apache.atlas.model.instance.AtlasAccessor;
import org.apache.atlas.type.AtlasTypeRegistry;

import java.util.List;
import java.util.Set;

public class AtlasAssetAccessorRequest extends AtlasAccessRequest {
    private List<AtlasAccessor> request;
    private AtlasTypeRegistry typeRegistry;

    public AtlasAssetAccessorRequest(List<AtlasAccessor> request, AtlasTypeRegistry typeRegistry) {
        super(null);
        this.request = request;
        this.typeRegistry = typeRegistry;
    }

    public Set<String> getEntityTypeAndAllSuperTypes(String typeName) {
        return super.getEntityTypeAndAllSuperTypes(typeName, typeRegistry);
    }

    public Set<String> getClassificationTypeAndAllSuperTypes(String classificationName) {
        return super.getClassificationTypeAndAllSuperTypes(classificationName, typeRegistry);
    }

    public List<AtlasAccessor> getRequest() {
        return request;
    }

    public void setRequest(List<AtlasAccessor> request) {
        this.request = request;
    }

    public AtlasTypeRegistry getTypeRegistry() {
        return typeRegistry;
    }

    public void setTypeRegistry(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }
}
