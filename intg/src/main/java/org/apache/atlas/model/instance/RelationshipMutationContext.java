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

package org.apache.atlas.model.instance;

import org.apache.commons.collections.CollectionUtils;
import org.apache.curator.shaded.com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

public final class RelationshipMutationContext {

    private final List<AtlasRelationship> createdRelationships;
    private final List<AtlasRelationship> updatedRelationships;
    private final List<AtlasRelationship> deletedRelationships;

    private RelationshipMutationContext(List<AtlasRelationship> createdRelationships, List<AtlasRelationship> updatedRelationships, List<AtlasRelationship> deletedRelationships) {
        this.createdRelationships = CollectionUtils.isNotEmpty(createdRelationships) ? ImmutableList.copyOf(createdRelationships) : new ArrayList<>();
        this.updatedRelationships = CollectionUtils.isNotEmpty(updatedRelationships) ? ImmutableList.copyOf(updatedRelationships) : new ArrayList<>();
        this.deletedRelationships = CollectionUtils.isNotEmpty(deletedRelationships) ? ImmutableList.copyOf(deletedRelationships) : new ArrayList<>();
    }

    public static RelationshipMutationContext getInstance(List<AtlasRelationship> createdRelationships, List<AtlasRelationship> updatedRelationships, List<AtlasRelationship> deletedRelationships) {
        return new RelationshipMutationContext(createdRelationships, updatedRelationships, deletedRelationships);
    }

    public List<AtlasRelationship> getCreatedRelationships() {
        return createdRelationships;
    }

    public List<AtlasRelationship> getUpdatedRelationships() {
        return updatedRelationships;
    }

    public List<AtlasRelationship> getDeletedRelationships() {
        return deletedRelationships;
    }
}
