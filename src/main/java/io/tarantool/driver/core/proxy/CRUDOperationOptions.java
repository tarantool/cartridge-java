package io.tarantool.driver.core.proxy;

import io.tarantool.driver.protocol.Packable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is not part of the public API.
 *
 * Represent options for proxy cluster operations
 *
 * @author Sergey Volgin
 */
public final class CRUDOperationOptions {

    public static final String TIMEOUT = "timeout";
    public static final String FIELDS = "fields";

    public static final String SELECT_LIMIT = "first";
    public static final String SELECT_AFTER = "after";
    public static final String SELECT_BATCH_SIZE = "batch_size";

    private final Integer timeout;
    private final List<String> fields;

    private final Long selectLimit;
    private final Long selectBatchSize;
    private final Packable after;

    private final Map<String, Object> resultMap = new HashMap<>(4, 1);

    private CRUDOperationOptions(Builder builder) {
        this.timeout = builder.timeout;
        this.fields = builder.fields;
        this.selectLimit = builder.selectLimit;
        this.after = builder.after;
        this.selectBatchSize = builder.selectBatchSize;
        initResultMap();
    }

    /**
     * Get a builder for this class.
     *
     * @return a new Builder for creating {@link CRUDOperationOptions}.
     */
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Integer timeout;
        private List<String> fields;
        private Long selectLimit;
        private Packable after;
        private Long selectBatchSize;

        public Builder() {
        }

        public Builder withTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder withFields(List<String> fields) {
            this.fields = fields;
            return this;
        }

        public Builder withSelectLimit(long selectLimit) {
            this.selectLimit = selectLimit;
            return this;
        }

        public Builder withSelectBatchSize(long selectBatchSize) {
            this.selectBatchSize = selectBatchSize;
            return this;
        }

        public Builder withSelectAfter(Packable startTuple) {
            this.after = startTuple;
            return this;
        }

        public CRUDOperationOptions build() {
            return new CRUDOperationOptions(this);
        }
    }

    private void initResultMap() {
        if (timeout != null) {
            resultMap.put(TIMEOUT, timeout);
        }

        if (fields != null) {
            resultMap.put(FIELDS, fields);
        }

        if (selectLimit != null) {
            resultMap.put(SELECT_LIMIT, selectLimit);
        }

        if (after != null) {
            resultMap.put(SELECT_AFTER, after);
        }

        if (selectBatchSize != null) {
            resultMap.put(SELECT_BATCH_SIZE, selectBatchSize);
        }
    }

    public Map<String, Object> asMap() {
        return resultMap;
    }
}
