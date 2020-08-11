package io.tarantool.driver.cluster;

import io.tarantool.driver.ServerAddress;

import java.util.Collection;

/**
 * Simple provider that produces a single connection.
 *
 * @author Sergey Volgin
 */
public class SingleAddressProvider implements AddressProvider {

    private ServerAddress serverAddress;

    public SingleAddressProvider(ServerAddress serverAddress) {
        this.serverAddress = serverAddress;
    }

    @Override
    public ServerAddress getAddress() {
        return serverAddress;
    }

    @Override
    public ServerAddress getNext() {
        return serverAddress;
    }

    /**
     * Update server address. Only first item will be used.
     *
     * @param addresses list {@link ServerAddress} of cluster nodes
     */
    @Override
    public void updateAddressList(Collection<ServerAddress> addresses) {
        if (addresses == null || addresses.size() == 0) {
            throw new IllegalArgumentException("At least one server address must be provided");
        }
        this.serverAddress = addresses.stream().findFirst().get();
    }
}
