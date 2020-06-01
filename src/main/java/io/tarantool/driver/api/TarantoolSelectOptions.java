package io.tarantool.driver.api;

import org.springframework.util.Assert;

/**
 * Represents common tuple selection options not related to the index and filtration, e.g. limit and offset
 *
 * @author Alexey Kuzin
 */
public class TarantoolSelectOptions {

    private static final long MAX_OFFSET = 0xff_ff_ff_ffL;
    private static final long MAX_LIMIT = 0xff_ff_ff_ffL;

    private long offset = 0L;
    private long limit = MAX_LIMIT;
    //TODO query timeouts

    public TarantoolSelectOptions() {
    }

    public long getOffset() {
        return offset;
    }

    private void setOffset(long offset) {
        this.offset = offset;
    }

    public long getLimit() {
        return limit;
    }

    private void setLimit(long limit) {
        this.limit = limit;
    }

    /**
     * Builder for Tarantool select options
     */
    public static class Builder {

        private TarantoolSelectOptions options;

        /**
         * Basic constructor
         */
        public Builder() {
            this.options = new TarantoolSelectOptions();
        }

        /**
         * Set how many tuples matching query must be skipped from the start
         * @param offset must be greater than 0 and less than 0xffffffff
         * @return Builder
         */
        public Builder withOffset(long offset) {
            Assert.state(offset >= 0 && offset <= MAX_OFFSET, "Offset mast be a value between 0 and 0xffffffff");

            options.setOffset(offset);
            return this;
        }

        /**
         * Set the maximum number of tuples matching query that must be included in the result
         * @param limit must be greater than 0 and less than 0xffffffff
         * @return Builder
         */
        public Builder withLimit(long limit) {
            Assert.state(limit >= 0 && limit <= MAX_LIMIT, "Limit mast be a value between 0 and 0xffffffff");

            options.setLimit(limit);
            return this;
        }

        public TarantoolSelectOptions build() {
            return options;
        }
    }
}
