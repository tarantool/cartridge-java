package io.tarantool.driver.api.metadata;

/**
 * Represents Tarantool index options
 *
 * @author Sergey Volgin
 */
public interface TarantoolIndexOptions {
    boolean isUnique();

    void setUnique(boolean unique);
}
