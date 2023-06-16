/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.model.authcache;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class AuthzCacheRefreshInfo {

    private boolean refreshPolicies;
    private boolean refreshRoles;
    private boolean refreshGroups;
    private boolean hardRefresh;

    public AuthzCacheRefreshInfo(Builder builder) {
        this.refreshPolicies = builder.refreshPolicies;
        this.refreshRoles = builder.refreshRoles;
        this.refreshGroups = builder.refreshGroups;
        this.hardRefresh = builder.hardRefresh;
    }

    public AuthzCacheRefreshInfo(boolean refreshPolicies, boolean refreshRoles,
                                  boolean refreshGroups, boolean hardRefresh) {
        this.refreshPolicies = refreshPolicies;
        this.refreshRoles = refreshRoles;
        this.refreshGroups = refreshGroups;
        this.hardRefresh = hardRefresh;
    }

    public static AuthzCacheRefreshInfo getDefaultTask() {
        return new AuthzCacheRefreshInfo(true, true, true, false);
    }

    public boolean isRefreshPolicies() {
        return refreshPolicies;
    }

    public boolean isRefreshRoles() {
        return refreshRoles;
    }

    public boolean isRefreshGroups() {
        return refreshGroups;
    }

    public boolean isHardRefresh() {
        return hardRefresh;
    }

    @Override
    public String toString() {
        return "{" +
                "refreshPolicies=" + refreshPolicies +
                ", refreshRoles=" + refreshRoles +
                ", refreshGroups=" + refreshGroups +
                ", hardRefresh=" + hardRefresh +
                "}";
    }

    public static class Builder {
        private boolean refreshPolicies = false;
        private boolean refreshRoles = false;
        private boolean refreshGroups = false;
        private boolean hardRefresh = false;

        public Builder setRefreshPolicies(boolean refreshPolicies) {
            this.refreshPolicies = refreshPolicies;
            return this;
        }

        public Builder setRefreshRoles(boolean refreshRoles) {
            this.refreshRoles = refreshRoles;
            return this;
        }

        public Builder setRefreshGroups(boolean refreshGroups) {
            this.refreshGroups = refreshGroups;
            return this;
        }

        public Builder setHardRefresh(boolean hardRefresh) {
            this.hardRefresh = hardRefresh;
            return this;
        }

        public AuthzCacheRefreshInfo build() {
            return new AuthzCacheRefreshInfo(this);
        }
    }
}
