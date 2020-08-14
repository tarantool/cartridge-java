package io.tarantool.driver.cluster;

import io.tarantool.driver.TarantoolServerAddress;

import java.util.Collection;

/**
 * Simple provider that produces a single connection.
 *
 * @author Sergey Volgin
 */
public class SingleAddressProvider implements AddressProvider, ServerSelectStrategy {

    private TarantoolServerAddress tarantoolServerAddress;

    public SingleAddressProvider(TarantoolServerAddress tarantoolServerAddress) {
        this.tarantoolServerAddress = tarantoolServerAddress;
    }

    @Override
    public TarantoolServerAddress getAddress() {
        return tarantoolServerAddress;
    }

    @Override
    public TarantoolServerAddress getNext() {
        return tarantoolServerAddress;
    }

    /**
     * Update server address. Only first item will be used.
     *
     * @param addresses list {@link TarantoolServerAddress} of cluster nodes
     */
    @Override
    public void updateAddressList(Collection<TarantoolServerAddress> addresses) {
        if (addresses == null || addresses.size() == 0) {
            throw new IllegalArgumentException("At least one server address must be provided");
        }
        this.tarantoolServerAddress = addresses.stream().findFirst().get();
    }
}
