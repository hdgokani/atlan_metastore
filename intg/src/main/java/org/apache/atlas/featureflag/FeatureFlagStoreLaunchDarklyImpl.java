package org.apache.atlas.featureflag;

import com.launchdarkly.sdk.LDContext;
import com.launchdarkly.sdk.server.LDClient;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

@Component
public class FeatureFlagStoreLaunchDarklyImpl implements FeatureFlagStore {

    private final LDClient client;

    @Inject
    public FeatureFlagStoreLaunchDarklyImpl(AtlasFeatureFlagClient client) {
        this.client = client.getClient();
    }

    @Override
    public boolean evaluate(FeatureFlag flag, String key, boolean value) {
        boolean ret;
        try {
            ret = client.boolVariation(flag.getKey(), getContext(key, value), flag.getDefaultValue());
        } catch (Exception e) {
            return false;
        }

        return ret;
    }

    @Override
    public boolean evaluate(FeatureFlag flag, String key, String value) {
        boolean ret;
        try {
            ret = client.boolVariation(flag.getKey(), getContext(key, value), flag.getDefaultValue());
        } catch (Exception e) {
            return false;
        }

        return ret;
    }

    private LDContext getContext(String key, String value) {
        LDContext ldContext = LDContext.builder(AtlasFeatureFlagClient.UNQ_CONTEXT_KEY)
                .name(AtlasFeatureFlagClient.CONTEXT_NAME)
                .set(key, value)
                .build();

        return ldContext;
    }

    private LDContext getContext(String key, boolean value) {
        LDContext ldContext = LDContext.builder(AtlasFeatureFlagClient.UNQ_CONTEXT_KEY)
                .name(AtlasFeatureFlagClient.CONTEXT_NAME)
                .set(key, value)
                .build();

        return ldContext;
    }
}