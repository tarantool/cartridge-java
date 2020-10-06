package io.tarantool.driver.api.cursor;

/**
 * Class-container config for batch cursor configuration.
 *
 * @author Sergey Volgin
 */
public class TarantoolBatchCursorOptions {

    public static final long DEFAULT_BATCH_SIZE = 100L;

    private final long batchSize;

    public TarantoolBatchCursorOptions() {
        this(DEFAULT_BATCH_SIZE);
    }

    public TarantoolBatchCursorOptions(long batchSize) {
        this.batchSize = batchSize;
    }

    public long getBatchSize() {
        return batchSize;
    }
}
