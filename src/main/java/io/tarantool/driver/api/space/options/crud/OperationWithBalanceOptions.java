package io.tarantool.driver.api.space.options.crud;

import java.util.Optional;

import io.tarantool.driver.api.space.options.Options;
import io.tarantool.driver.api.space.options.Self;
import io.tarantool.driver.api.space.options.crud.enums.ProxyOption;

/**
 * Base interface for all operation options that may have a configurable "balance" option.
 *
 * @author Belonogov Nikolay
 */
public interface OperationWithBalanceOptions<T extends OperationWithBalanceOptions<T>> extends Options, Self<T> {

    /**
     * Sets "balance" option value to true. If true then use replica according to vshard load balancing policy. Default
     * value is false.
     *
     * @return this options instance.
     */
    default T balance() {
        addOption(ProxyOption.BALANCE, true);
        return self();
    }

    /**
     * @return "balance" option value.
     */
    default Optional<Boolean> getBalance() {
        return getOption(ProxyOption.BALANCE, Boolean.class);
    }
}
