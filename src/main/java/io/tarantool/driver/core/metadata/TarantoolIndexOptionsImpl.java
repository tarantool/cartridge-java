package io.tarantool.driver.core.metadata;

import io.tarantool.driver.api.metadata.TarantoolIndexOptions;

/**
 * Represents Tarantool index options
 *
 * @author Sergey Volgin
 */
class TarantoolIndexOptionsImpl implements TarantoolIndexOptions {

    private boolean isUnique;

    TarantoolIndexOptionsImpl() {
    }

    @Override
    public boolean isUnique() {
        return isUnique;
    }

    @Override
    public void setUnique(boolean unique) {
        isUnique = unique;
    }
}
