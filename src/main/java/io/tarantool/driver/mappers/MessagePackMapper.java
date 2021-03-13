package io.tarantool.driver.mappers;

/**
 * Combines both ObjectMapper and ValueMapper interfaces
 *
 * @author Alexey Kuzin
 */
public interface MessagePackMapper extends MessagePackObjectMapper, MessagePackValueMapper {
    /**
     * Makes a shallow copy of this mapper instance
     *
     * @return new mapper instance
     */
    MessagePackMapper copy();
}
