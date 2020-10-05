package io.tarantool.driver.proxy;

import io.tarantool.driver.api.tuple.TarantoolTuple;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is not part of the public API.
 *
 * Represent options for proxy cluster operations
 *
 * @author Sergey Volgin
 */
public final class CRUDOperationOptions {

    public static final String TIME_OUT = "timeout";
    public static final String TUPLES_TO_MAP = "tuples_tomap";

    public static final String SELECT_LIMIT = "limit";
    public static final String SELECT_AFTER = "after";
    public static final String SELECT_BATCH_SIZE = "batch_size";

    private final Integer timeout;
    private final Boolean tuplesAsMap;

    private final Long selectLimit;
    private final Long selectBatchSize;
    private final TarantoolTuple alter;

    private final Map<String, Object> resultMap = new HashMap<>(5, 1);

    private CRUDOperationOptions(Builder builder) {
        this.timeout = builder.timeout;
        this.tuplesAsMap = builder.tuplesToMap;

        this.selectLimit = builder.selectLimit;
        this.alter = builder.alter;
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
        private Boolean tuplesToMap;

        private Long selectLimit;
        private TarantoolTuple alter;
        private Long selectBatchSize;

        public Builder() {
        }

        public Builder withTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder withTuplesToMap(boolean tuplesToMap) {
            this.tuplesToMap = tuplesToMap;
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

        public Builder withSelectAfter(TarantoolTuple tuple) {
            this.alter = tuple;
            return this;
        }

        public CRUDOperationOptions build() {
            return new CRUDOperationOptions(this);
        }
    }

    private void initResultMap() {
        if (timeout != null) {
            resultMap.put(TIME_OUT, timeout);
        }

        if (tuplesAsMap != null) {
            resultMap.put(TUPLES_TO_MAP, tuplesAsMap);
        }

        if (selectLimit != null) {
            resultMap.put(SELECT_LIMIT, selectLimit);
        }

        if (alter != null) {
            resultMap.put(SELECT_AFTER, alter);
        }

        if (selectBatchSize != null) {
            resultMap.put(SELECT_BATCH_SIZE, selectBatchSize);
        }
    }

    public Map<String, Object> asMap() {
        return resultMap;
    }
}
