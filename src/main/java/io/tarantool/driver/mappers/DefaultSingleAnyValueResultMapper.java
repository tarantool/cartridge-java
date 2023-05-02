package io.tarantool.driver.mappers;

import io.tarantool.driver.api.SingleValueCallResult;

/**
 * Universal mapper for {@link SingleValueCallResult} if content type is uncertain
 *
 * @author Artyom Dubinin
 */
public class DefaultSingleAnyValueResultMapper extends DefaultSingleValueResultMapper<Object> {

    public DefaultSingleAnyValueResultMapper(MessagePackValueMapper valueMapper) {
        super(valueMapper, Object.class);
    }
}
