package io.tarantool.driver.mappers;

import java.io.Serializable;

/**
 * Combines both ObjectMapper and ValueMapper interfaces
 *
 * @author Alexey Kuzin
 */
public interface MessagePackMapper
        extends MessagePackObjectMapper, MessagePackValueMapper, Serializable {
    /**
     * Makes a shallow copy of this mapper instance
     *
     * @return new mapper instance
     */
    MessagePackMapper copy();
}
