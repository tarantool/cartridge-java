package io.tarantool.driver.api.space.options.contracts;

import io.tarantool.driver.api.space.options.enums.ProxyOption;
import io.tarantool.driver.api.space.options.interfaces.Options;
import io.tarantool.driver.api.space.options.interfaces.Self;
import io.tarantool.driver.protocol.Packable;

import java.util.Optional;

/**
 * Base interface for all operation options that may have a configurable after.
 *
 * @param <T> type of OperationWith.
 * @author <a href="https://github.com/nickkkccc">Belonogov Nikolay</a>
 */

public interface OperationWithAfterOptions<T extends OperationWithAfterOptions<T>> extends Options, Self<T> {

    /**
     * Adds the tuple after which the selection occurs in the select operation.
     *
     * @param after the tuple after which the selection occurs in the select operation.
     * @return type of this class.
     */
    default T withAfter(Packable after) {
        addOption(ProxyOption.AFTER, after);
        return self();
    }

    /**
     * Returns the tuple after which the selection occurs in the select operation.
     *
     * @return after tuple.
     */
    default Optional<Packable> getAfter() {
        return getOption(ProxyOption.AFTER, Packable.class);
    }
}
