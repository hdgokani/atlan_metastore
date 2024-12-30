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

package org.apache.atlas.model.audit;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.collections.CollectionUtils;

import java.util.*;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)

public class AuditSearchParams {

    private static Map<String, Object> defaultSort = new HashMap<>();

    private Map dsl;
    private Set<String> attributes;

    private boolean suppressLogs;

    public AuditSearchParams() {
        Map<String, Object> order = new HashMap<>();
        order.put("order", "desc");

        defaultSort.put("created", order);
        suppressLogs = true;
    }

    public void setDsl(Map dsl) {
        this.dsl = dsl;
    }

    public Map getDsl() {
        return this.dsl;
    }

    public boolean getSuppressLogs() {
        return suppressLogs;
    }

    public void setSuppressLogs(boolean suppressLogs) {
        this.suppressLogs = suppressLogs;
    }

    public String getQueryStringForGuid(String guid) {
        String queryWithEntityFilter;
        if (dsl.get("query") == null || ((Map) dsl.get("query")).isEmpty()) {
            String queryTemplate = "{\"bool\":{\"minimum_should_match\":\"100%\",\"should\":[{\"term\":{\"entityId.keyword\":\"entity_id\"}}]}}";
            queryWithEntityFilter = queryTemplate.replace("entity_id", guid);
        } else {
            String queryTemplate = "{\"bool\":{\"minimum_should_match\":\"100%\",\"should\":[{\"term\":{\"entityId.keyword\":\"entity_id\"}}, query_from_payload]}}";
            queryWithEntityFilter = queryTemplate.replace("entity_id", guid);
            String queryValue = AtlasType.toJson(dsl.get("query"));
            queryWithEntityFilter = queryWithEntityFilter.replace("query_from_payload", queryValue);
        }
        dsl.put("query", AtlasType.fromJson(queryWithEntityFilter, Map.class));
        return AtlasType.toJson(dsl);
    }

    public String getQueryString() {
        if (this.dsl != null) {
            if (!this.dsl.containsKey("sort")) {
                dsl.put("sort", Collections.singleton(defaultSort));
            }
            return AtlasType.toJson(dsl);
        }
        return "";
    }

    public Set<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Set<String> attributes) {
        this.attributes = attributes;
    }
}
