package io.tarantool.driver.api.space.options.crud;

import java.util.Optional;

import io.tarantool.driver.api.space.options.Options;
import io.tarantool.driver.api.space.options.Self;
import io.tarantool.driver.api.space.options.crud.enums.ProxyOption;

/**
 * Base interface for all operation options that may have a configurable "prefer_replica".
 *
 * @author Belonogov Nikolay
 */
public interface OperationWithPreferReplicaOptions<T extends OperationWithPreferReplicaOptions<T>>
    extends Options, Self<T> {

    /**
     * Sets "prefer_replica" option value to true. If true then the preferred target is one of the replicas.
     * Default value is false.
     *
     * @return this options instance.
     */
    default T preferReplica() {
        addOption(ProxyOption.PREFER_REPLICA, true);
        return self();
    }

    /**
     * @return "prefer_replica" option value.
     */
    default Optional<Boolean> getPreferReplica() {
        return getOption(ProxyOption.PREFER_REPLICA, Boolean.class);
    }
}
