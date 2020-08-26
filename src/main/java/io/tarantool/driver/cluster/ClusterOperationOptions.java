package io.tarantool.driver.cluster;

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
public final class ClusterOperationOptions {

    public static final String TIME_OUT = "timeout";
    public static final String TUPLES_AS_MAP = "tuples_as_map";
    public static final String SELECT_KEY = "key";
    public static final String SELECT_LIMIT = "limit";
    public static final String SELECT_OFFSET = "offset";
    public static final String SELECT_ITERATOR = "iterator";
    public static final String SELECT_BATCH_SIZE = "batch_size";

    private Integer timeout;
    private Boolean tuplesAsMap;
    private List<?> selectKey;
    private Long selectLimit;
    private Long selectOffset;
    private String selectIterator;
    private Long selectBatchSize;

    private ClusterOperationOptions() {
    }

    private ClusterOperationOptions(Builder builder) {
        this.timeout = builder.timeout;
        this.tuplesAsMap = builder.tuplesAsMap;
        this.selectKey = builder.selectKey;
        this.selectLimit = builder.selectLimit;
        this.selectOffset = builder.selectOffset;
        this.selectIterator = builder.selectIterator;
        this.selectBatchSize = builder.selectBatchSize;
    }

    /**
     * Get a builder for this class.
     *
     * @return a new Builder for creating {@link ClusterOperationsMappingConfig}.
     */
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Integer timeout;
        private Boolean tuplesAsMap;
        private List<?> selectKey;
        private Long selectLimit;
        private Long selectOffset;
        private String selectIterator;
        private Long selectBatchSize;

        public Builder() {
        }

        public Builder withTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder withTuplesAsMap(boolean tuplesAsMap) {
            this.tuplesAsMap = tuplesAsMap;
            return this;
        }

        public Builder withSelectKey(List<?> selectKey) {
            this.selectKey = selectKey;
            return this;
        }

        public Builder withSelectLimit(long selectLimit) {
            this.selectLimit = selectLimit;
            return this;
        }

        public Builder withSelectOffset(long selectOffset) {
            this.selectOffset = selectOffset;
            return this;
        }

        public Builder withSelectIterator(String selectIterator) {
            this.selectIterator = selectIterator;
            return this;
        }

        public Builder withSelectBatchSize(long selectBatchSize) {
            this.selectBatchSize = selectBatchSize;
            return this;
        }

        public ClusterOperationOptions build() {
            return new ClusterOperationOptions(this);
        }
    }

    public Map<String, Object> asMap() {
        Map<String, Object> result = new HashMap<>();

        if (timeout != null) {
            result.put(TIME_OUT, timeout);
        }

        if (tuplesAsMap != null) {
            result.put(TUPLES_AS_MAP, tuplesAsMap);
        }

        if (selectKey != null) {
            result.put(SELECT_KEY, selectKey);
        }

        if (selectLimit != null) {
            result.put(SELECT_LIMIT, selectLimit);
        }

        if (selectOffset != null) {
            result.put(SELECT_OFFSET, selectOffset);
        }

        if (selectIterator != null) {
            result.put(SELECT_ITERATOR, selectIterator);
        }

        if (selectBatchSize != null) {
            result.put(SELECT_BATCH_SIZE, selectBatchSize);
        }

        return result;
    }
}
