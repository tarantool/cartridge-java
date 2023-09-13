package io.tarantool.driver.api.space.options.contracts;

import io.tarantool.driver.api.space.options.enums.ProxyOption;
import io.tarantool.driver.api.space.options.interfaces.Options;
import io.tarantool.driver.api.space.options.interfaces.Self;

import java.util.Optional;

/**
 * Base interface for all operation options that may have a configurable first.
 *
 * @param <T> type of this class.
 * @author <a href="https://github.com/nickkkccc">Belonogov Nikolay</a>
 */
public interface OperationWithFirstOptions<T extends OperationWithFirstOptions<T>> extends Options, Self<T> {

    /**
     * Adds limit for select operation.
     *
     * @param first value of limit.
     * @return type of this class.
     */
    default T withFirst(long first) {
        addOption(ProxyOption.FIRST, first);
        return self();
    }

    /**
     * Returns value of limit for select option.
     *
     * @return value of limit.
     */
    default Optional<Long> getFirst() {
        return getOption(ProxyOption.FIRST, Long.class);
    }
}
