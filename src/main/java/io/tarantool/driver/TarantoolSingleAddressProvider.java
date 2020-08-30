package io.tarantool.driver;

/**
 * Provides a single address for connecting to Tarantool instance
 *
 * @author Sergey Volgin
 * @author Alexey Kuzin
 */
public interface TarantoolSingleAddressProvider {
    TarantoolServerAddress getAddress();
}
