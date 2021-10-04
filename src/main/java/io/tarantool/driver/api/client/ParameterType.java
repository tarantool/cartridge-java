package io.tarantool.driver.api.client;

import java.util.Arrays;
import java.util.List;

/**
 * Provides parameters types and groups for Tarantool client builder.
 * <p>
 * @see TarantoolClientBuilderImpl
 *
 * @author Oleg Kuznetsov
 */
public enum ParameterType {

    PROXY_MAPPING,
    RETRY_ATTEMPTS,
    RETRY_DELAY,
    REQUEST_TIMEOUT,
    EXCEPTION_CALLBACK,
    OPERATION_TIMEOUT;

    /**
     * Group of {@link ParameterType} parameters
     */
    public enum ParameterGroup {
        PROXY(PROXY_MAPPING),
        RETRY(RETRY_ATTEMPTS, RETRY_DELAY, REQUEST_TIMEOUT, EXCEPTION_CALLBACK, OPERATION_TIMEOUT);

        ParameterGroup(ParameterType... type) {
            this.types = Arrays.asList(type);
        }

        private final List<ParameterType> types;

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
