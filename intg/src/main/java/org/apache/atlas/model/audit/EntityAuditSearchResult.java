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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.model.Clearable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.ALWAYS)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class EntityAuditSearchResult implements Serializable, Clearable  {

    public EntityAuditSearchResult() { }

    private List<EntityAuditEventV2> entityAudits;
    private Map<String, Object> aggregations;
    private int count;
    private int totalCount;

    public List<EntityAuditEventV2> getEntityAudits() {
        return entityAudits;
    }

    public void setEntityAudits(List<EntityAuditEventV2> entityAudits) {
        this.entityAudits = entityAudits;
    }

    public Map<String, Object> getAggregations() {
        return aggregations;
    }

    public void setAggregations(Map<String, Object> aggregations) {
        this.aggregations = aggregations;
    }

    public long getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        EntityAuditSearchResult that = (EntityAuditSearchResult) o;

        return Objects.equals(entityAudits, that.entityAudits) &&
                Objects.equals(aggregations, that.aggregations) &&
                Objects.equals(count, that.count) &&
                Objects.equals(totalCount, that.totalCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityAudits, aggregations, count, totalCount);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("EntityAuditSearchResult{");
        sb.append("entityAudits='").append(entityAudits).append('\'');
        sb.append(", aggregations='").append(aggregations).append('\'');
        sb.append(", count=").append(count);
        sb.append(", totalCount=").append(totalCount);
        sb.append('}');

        return sb.toString();
    }

    @Override
    public void clear() {
        entityAudits = null;
        aggregations = null;
        count = 0;
        totalCount = 0;
    }
}
