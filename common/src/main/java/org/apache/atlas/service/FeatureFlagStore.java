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

package org.apache.atlas.service;

import org.apache.atlas.service.redis.RedisService;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

@Component
public class FeatureFlagStore {
    private static RedisService redisService = null;

    @Inject
    public FeatureFlagStore(RedisService redisService) {
        FeatureFlagStore.redisService = redisService;
    }

    public static boolean evaluate(String key, String expectedValue) {
        boolean ret = false;
        try{
            if (StringUtils.isEmpty(key) || StringUtils.isEmpty(expectedValue))
                return ret;
            String value = redisService.getValue(addFeatureFlagNamespace(key));
            ret = StringUtils.equals(value, expectedValue);
        } catch (Exception e) {
            return ret;
        }
        return ret;
    }

    public static void setFlag(String key, String value) {
        if (StringUtils.isEmpty(key) || StringUtils.isEmpty(value))
            return;

        redisService.putValue(addFeatureFlagNamespace(key), value);
    }

    public static void deleteFlag(String key) {
        if (StringUtils.isEmpty(key))
            return;

        redisService.removeValue(addFeatureFlagNamespace(key));
    }

    private static String addFeatureFlagNamespace(String key) {
        return "ff:"+key;
    }
}
