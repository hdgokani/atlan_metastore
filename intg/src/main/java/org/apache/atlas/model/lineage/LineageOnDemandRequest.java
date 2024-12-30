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

package org.apache.atlas.model.lineage;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.model.discovery.SearchParameters;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class LineageOnDemandRequest {
    private Map<String, LineageOnDemandConstraints> constraints;
    @JsonAlias({"traversalFilters", "entityTraversalFilters"}) // TODO: Will be deprecated after FE changes.
    private SearchParameters.FilterCriteria         entityTraversalFilters;
    private SearchParameters.FilterCriteria         relationshipTraversalFilters;
    private Set<String>                             attributes;
    private Set<String>                             relationAttributes;
    private LineageOnDemandBaseParams               defaultParams;

    public LineageOnDemandRequest() {
        this.attributes = new HashSet<>();
        this.relationAttributes = new HashSet<>();
        this.defaultParams = new LineageOnDemandBaseParams();
    }

    public LineageOnDemandRequest(Map<String, LineageOnDemandConstraints> constraints, SearchParameters.FilterCriteria entityTraversalFilters,
                                  SearchParameters.FilterCriteria relationshipTraversalFilters, Set<String> attributes,
                                  Set<String> relationAttributes, LineageOnDemandBaseParams defaultParams) {
        this.constraints                  = constraints;
        this.entityTraversalFilters       = entityTraversalFilters;
        this.relationshipTraversalFilters = relationshipTraversalFilters;
        this.attributes                   = attributes;
        this.relationAttributes           = relationAttributes;
        this.defaultParams                = defaultParams;
    }

    public Map<String, LineageOnDemandConstraints> getConstraints() {
        return constraints;
    }

    public void setConstraints(Map<String, LineageOnDemandConstraints> constraints) {
        this.constraints = constraints;
    }

    public SearchParameters.FilterCriteria getEntityTraversalFilters() {
        return entityTraversalFilters;
    }

    public void setEntityTraversalFilters(SearchParameters.FilterCriteria entityTraversalFilters) {
        this.entityTraversalFilters = entityTraversalFilters;
    }

    public SearchParameters.FilterCriteria getRelationshipTraversalFilters() {
        return relationshipTraversalFilters;
    }

    public void setRelationshipTraversalFilters(SearchParameters.FilterCriteria relationshipTraversalFilters) {
        this.relationshipTraversalFilters = relationshipTraversalFilters;
    }

    public Set<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Set<String> attributes) {
        this.attributes = attributes;
    }

    public Set<String> getRelationAttributes() {
        return relationAttributes;
    }

    public void setRelationAttributes(Set<String> relationAttributes) {
        this.relationAttributes = relationAttributes;
    }

    public LineageOnDemandBaseParams getDefaultParams() {
        return defaultParams;
    }

    public void setDefaultParams(LineageOnDemandBaseParams defaultParams) {
        this.defaultParams = defaultParams;
    }
}
