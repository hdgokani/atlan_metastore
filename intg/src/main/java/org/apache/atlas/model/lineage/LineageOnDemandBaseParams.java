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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.AtlasConfiguration;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class LineageOnDemandBaseParams {
    private int              inputRelationsLimit;
    private int              outputRelationsLimit;

    public static final int LINEAGE_ON_DEMAND_DEFAULT_NODE_COUNT = AtlasConfiguration.LINEAGE_ON_DEMAND_DEFAULT_NODE_COUNT.getInt();

    public LineageOnDemandBaseParams() {
        this.inputRelationsLimit = LINEAGE_ON_DEMAND_DEFAULT_NODE_COUNT;
        this.outputRelationsLimit = LINEAGE_ON_DEMAND_DEFAULT_NODE_COUNT;
    }

    public LineageOnDemandBaseParams(int inputRelationsLimit, int outputRelationsLimit) {
        this.inputRelationsLimit = inputRelationsLimit;
        this.outputRelationsLimit = outputRelationsLimit;
    }

    public int getInputRelationsLimit() {
        return inputRelationsLimit;
    }

    public void setInputRelationsLimit(int inputRelationsLimit) {
        this.inputRelationsLimit = inputRelationsLimit;
    }

    public int getOutputRelationsLimit() {
        return outputRelationsLimit;
    }

    public void setOutputRelationsLimit(int outputRelationsLimit) {
        this.outputRelationsLimit = outputRelationsLimit;
    }
}