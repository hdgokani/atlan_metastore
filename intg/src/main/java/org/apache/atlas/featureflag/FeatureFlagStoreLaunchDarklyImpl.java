package org.apache.atlas.featureflag;

import com.launchdarkly.sdk.LDContext;
import com.launchdarkly.sdk.server.LDClient;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.Objects;

import static org.apache.atlas.featureflag.AtlasFeatureFlagClient.INSTANCE_DOMAIN_NAME;

@Service
public class FeatureFlagStoreLaunchDarklyImpl implements FeatureFlagStore {

    public final static String INSTANCE_DOMAIN_KEY = "instance";

    private final LDClient client;

    private final static LDContext LD_CONTEXT = LDContext.builder(AtlasFeatureFlagClient.UNQ_CONTEXT_KEY)
            .name(AtlasFeatureFlagClient.CONTEXT_NAME)
            .set(INSTANCE_DOMAIN_KEY, AtlasFeatureFlagClient.INSTANCE_DOMAIN_NAME)
            .build();

    public FeatureFlagStoreLaunchDarklyImpl() {
        this.client = AtlasFeatureFlagClient.getClient();
    }

    @Override
    public boolean evaluate(FeatureFlag flag, boolean featureFlagValue) {
        boolean ret;
        try {
            boolean defaultValue = (Boolean) flag.getDefaultValue();

            ret = client.boolVariation(flag.getKey(), LD_CONTEXT, defaultValue);
        } catch (Exception e) {
            return false;
        }

        return ret;
    }
}