package io.tarantool.driver.api.cursor;

import java.io.Serializable;

/**
 * Class-container config for batch cursor configuration.
 *
 * @author Sergey Volgin
 */
public class TarantoolCursorOptions implements Serializable {
    private static final long serialVersionUID = 2251758036553381849L;

    public static final long DEFAULT_BATCH_SIZE = 100L;

    private final long batchSize;

    public TarantoolCursorOptions() {
        this(DEFAULT_BATCH_SIZE);
    }

    public TarantoolCursorOptions(long batchSize) {
        this.batchSize = batchSize;
    }

    public long getBatchSize() {
        return batchSize;
    }
}
