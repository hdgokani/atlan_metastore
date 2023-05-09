package org.apache.atlas.featureflag;

public interface FeatureFlagStore {

    String DISABLE_ACCESS_CONTROL_FEATURE_FLAG_KEY = "disable-access-control-ops";

    boolean evaluate(FeatureFlag flag, boolean featureFlagValue);

    enum FeatureFlag {
        DISABLE_ACCESS_CONTROL(DISABLE_ACCESS_CONTROL_FEATURE_FLAG_KEY, false);

        private final String key;
        private final Object defaultValue;

        FeatureFlag(String key, Object defaultValue) {
            this.key = key;
            this.defaultValue = defaultValue;
        }

        public String getKey() {
            return key;
        }

        public Object getDefaultValue() {
            return defaultValue;
        }
    }
}