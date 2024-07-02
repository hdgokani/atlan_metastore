package org.apache.atlas.service.redis;

import org.apache.atlas.AtlasException;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.redisson.Redisson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.TimeUnit;

@Component("redisServiceImpl")
@ConditionalOnAtlasProperty(property = "atlas.redis.service.impl")
public class RedisServiceLocalImpl extends AbstractRedisService {

    private static final Logger LOG = LoggerFactory.getLogger(RedisServiceLocalImpl.class);

    @PostConstruct
    public void init() throws AtlasException {
        redisClient = Redisson.create(getLocalConfig());
        redisCacheClient = Redisson.create(getLocalConfig());
        LOG.info("Local redis client created successfully.");
    }

    @Override
    public String getValue(String key) {

        // If value doesn't exist, return null else return the value
        return (String) redisCacheClient.getBucket(convertToNamespace(key)).get();
    }

    @Override
    public String putValue(String key, String value) {
        // Put the value in the redis cache with TTL
        redisCacheClient.getBucket(convertToNamespace(key)).set(value);
        return value;
    }

    @Override
    public String putValue(String key, String value, int timeout) {
        // Put the value in the redis cache with TTL
        redisCacheClient.getBucket(convertToNamespace(key)).set(value, timeout, TimeUnit.SECONDS);
        return value;
    }

    @Override
    public void removeValue(String key)  {
        // Remove the value from the redis cache
        redisCacheClient.getBucket(convertToNamespace(key)).delete();
    }

    private String convertToNamespace(String key){
        // Append key with namespace :atlas
        return "atlas:"+key;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
