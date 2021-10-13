package io.tarantool.driver.core.metadata;

/**
 * Represents Tarantool index options
 *
 * @author Sergey Volgin
 */
public class TarantoolIndexOptions {

    private boolean isUnique;

    public TarantoolIndexOptions() {
    }

    public boolean isUnique() {
        return isUnique;
    }

    public void setUnique(boolean unique) {
        isUnique = unique;
    }
}
