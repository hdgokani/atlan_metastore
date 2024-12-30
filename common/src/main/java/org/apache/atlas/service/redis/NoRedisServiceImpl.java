/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.service.redis;

import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
@ConditionalOnAtlasProperty(property = "atlas.redis.service.impl", isDefault = true)
public class NoRedisServiceImpl extends AbstractRedisService {

    private static final Logger LOG = LoggerFactory.getLogger(NoRedisServiceImpl.class);

    @PostConstruct
    public void init() {
        LOG.info("Enabled no redis implementation.");
    }

    @Override
    public boolean acquireDistributedLock(String key) {
        //do nothing
        return true;
    }

    @Override
    public void releaseDistributedLock(String key) {
        //do nothing
    }

    @Override
    public String getValue(String key) {
        return null;
    }

    @Override
    public String putValue(String key, String value, int timeout) {
        return null;
    }

    @Override
    public void removeValue(String key) {

    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

}
