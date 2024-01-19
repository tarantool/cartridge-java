package io.tarantool.driver.api.space.options.crud;

import java.util.Optional;

import io.tarantool.driver.api.space.options.Options;
import io.tarantool.driver.api.space.options.Self;
import io.tarantool.driver.api.space.options.crud.enums.ProxyOption;

public interface OperationWithYieldEveryOptions<T extends OperationWithYieldEveryOptions<T>>
    extends Options, Self<T> {

    /**
     * Sets number of tuples processed on storage to yield after, "yield_every" should be > 0.
     * @param yieldEvery number of tuples processed on storage to yield after, "yield_every" should be > 0.
     * @return this option instance.
     * @throws IllegalArgumentException if yieldEvery < 0.
     */
    default T withYieldEvery(int yieldEvery) throws IllegalArgumentException {
        if (yieldEvery <= 0) {
            throw new IllegalArgumentException("Parameter \"yield_every\" must be greater than 0");
        }
        addOption(ProxyOption.YIELD_EVERY, yieldEvery);
        return self();
    }

    /**
     * @return "yield_every" parameter value.
     */
    default Optional<Integer> getYieldEvery() {
        return getOption(ProxyOption.BUCKET_ID, Integer.class);
    }
}
