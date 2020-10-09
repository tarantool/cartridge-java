package io.tarantool.driver.cluster;

import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.TarantoolServerAddress;
import org.testcontainers.containers.TarantoolCartridgeContainer;

import java.util.Collection;
import java.util.stream.Collectors;

public class TestWrappedClusterAddressProvider implements TarantoolClusterAddressProvider {

    private final TarantoolClusterAddressProvider provider;
    private final TarantoolCartridgeContainer container;

    public TestWrappedClusterAddressProvider(TarantoolClusterAddressProvider provider,
                                             TarantoolCartridgeContainer container) {
        this.provider = provider;
        this.container = container;
    }

    @Override
    public Collection<TarantoolServerAddress> getAddresses() {
        Collection<TarantoolServerAddress> addresses = provider.getAddresses();

        return addresses.stream().map(a ->
            new TarantoolServerAddress(
                    a.getHost(),
                    container.getMappedPort(a.getPort())
            )
        ).collect(Collectors.toList());
    }
}
