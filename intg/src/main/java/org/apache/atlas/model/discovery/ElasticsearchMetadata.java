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

package org.apache.atlas.model.discovery;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.commons.collections.MapUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

public class ElasticsearchMetadata {

    private Map<String, List<String>> highlights;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private ArrayList<Object> sort;

    public Map<String, List<String>> getHighlights() {
        return highlights;
    }

    public void addHighlights(Map<String, List<String>> highlights) {
        if(MapUtils.isNotEmpty(highlights)) {
            if (MapUtils.isEmpty(this.highlights)) {
                this.highlights = new HashMap<>();
            }
            this.highlights.putAll(highlights);
        }
    }

    public Object getSort() { return sort; }

    public void addSort(ArrayList<Object> sort) {
        if (sort.isEmpty()) {
            this.sort = null;
        } else {
            this.sort = sort;
        }
    }

    @Override
    public String toString() {
        return "SearchMetadata{" +
                "highlights=" + highlights +
                '}';
    }
}
