package io.tarantool.driver.api.client;

import java.util.Arrays;
import java.util.List;

/**
 * Parameters types for creating tarantool client
 * Used in {@link TarantoolClientBuilderImpl} for collection and sorting user parameters
 */
public enum ParameterType {

    CREDENTIALS,
    ADDRESS,
    CONNECTION_SELECTION_STRATEGY,
    PROXY_MAPPING,
    RETRY_ATTEMPTS,
    RETRY_DELAY,
    REQUEST_TIMEOUT,
    EXCEPTION_CALLBACK;

    /**
     * Group of {@link ParameterType} parameters
     */
    public enum ParameterGroup {
        BASIC(CREDENTIALS, ADDRESS, CONNECTION_SELECTION_STRATEGY),
        RETRY(RETRY_ATTEMPTS, RETRY_DELAY, REQUEST_TIMEOUT, EXCEPTION_CALLBACK),
        PROXY(PROXY_MAPPING);

        final private List<ParameterType> types;

        ParameterGroup(ParameterType... type) {
            this.types = Arrays.asList(type);
        }

        /**
         * @return list of parameters types in group
         */
        public List<ParameterType> getTypes() {
            return types;
        }

        /**
         * Check for containing parameter type in group
         *
         * @param type {@link ParameterType}
         * @return true if parameter contains in parameter group, return false if not
         */
        public boolean hasParameterType(ParameterType type) {
            return types.contains(type);
        }
    }
}
