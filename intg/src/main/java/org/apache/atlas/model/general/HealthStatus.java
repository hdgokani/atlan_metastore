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

package org.apache.atlas.model.general;

import org.apache.atlas.type.AtlasType;

public class HealthStatus {
    public String name;
    public String status;
    public boolean fatal;
    public String checkTime;
    public String errorString;

    public HealthStatus(String name, String status, boolean fatal, String checkTime, String errorString) {
        this.name = name;
        this.status = status;
        this.fatal = fatal;
        this.checkTime = checkTime;
        this.errorString = errorString;
    }

    @Override
    public String toString() {
        return "HealthStatus{" +
                "name=" + name +
                ", status='" + status +
                ", fatal=" + fatal +
                ", checkTime=" + checkTime +
                ", errorString=" + errorString +
                '}';
    }
}
