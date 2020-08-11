package io.tarantool.driver.cluster;

import io.tarantool.driver.ServerAddress;
import io.tarantool.driver.TarantoolClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolDaemonThreadFactory;
import io.tarantool.driver.exceptions.TarantoolClientException;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinAddressProvider implements AutoCloseable, AddressProvider, AddressProviderWithClusterDiscovery {

    private final List<ServerAddress> addressList = new ArrayList<>();
    private AtomicInteger currentPosition = new AtomicInteger(-1);
    private ScheduledExecutorService scheduledExecutorService;

    /**
     * Constructs an instance.
     *
     * @param addresses optional array of addresses in a form of host[:port]
     *
     * @throws IllegalArgumentException if addresses aren't provided
     */
    public RoundRobinAddressProvider(List<ServerAddress> addresses) {
        updateAddressList(addresses);
    }

    @Override
    public void runClusterDiscovery(TarantoolClientConfig config, TarantoolClient tarantoolClient)
            throws TarantoolClientException {
        this.scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(new TarantoolDaemonThreadFactory("tarantool-discovery"));
        this.createDiscoveryTask(config, tarantoolClient);
    }

    @Override
    public ServerAddress getAddress() {
        synchronized (addressList) {
            if (addressList.size() > 0 && currentPosition.get() >= 0) {
                int position = currentPosition.get() % addressList.size();
                return addressList.get(position);
            } else {
                return null;
            }
        }
    }

    @Override
    public ServerAddress getNext() {
        synchronized (addressList) {
            int position = currentPosition.updateAndGet(i -> (i + 1) % addressList.size());
            return addressList.get(position);
        }
    }

    /**
     * Update provider address list. A duplicate server addresses are removed from the list.
     * @param addresses list {@link ServerAddress} of cluster nodes
     */
    @Override
    public void updateAddressList(Collection<ServerAddress> addresses) {
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalArgumentException("At least one server address must be provided");
        }
        synchronized (addressList) {
            ServerAddress currentAddress = getAddress();
            Set<ServerAddress> hostsSet = new LinkedHashSet<>(addresses.size());
            addressList.clear();

            for (ServerAddress serverAddress : addresses) {
                Assert.notNull(serverAddress, "ServerAddress must not be null");
                hostsSet.add(new ServerAddress(serverAddress.getHost(), serverAddress.getPort()));
            }

            addressList.addAll(hostsSet);

            int newPosition = addressList.indexOf(currentAddress);
            currentPosition.set(newPosition);
        }
    }

    /**
     * Get number of {@link ServerAddress} in the pool
     * @return number of {@link ServerAddress} in the pool
     */
    public int size() {
        synchronized (addressList) {
            return addressList.size();
        }
    }

    private void createDiscoveryTask(TarantoolClientConfig config, TarantoolClient tarantoolClient)
            throws TarantoolClientException {
        ClusterDiscoverer clusterDiscoverer =
                ClusterDiscovererFactory.create(config.getClusterDiscoveryEndpoint(), tarantoolClient, config);

        Runnable discoveryTask = () -> {
            try {
                List<ServerAddress> addresses = clusterDiscoverer.getNodes();
                this.updateAddressList(addresses);
            } catch (Exception ignored) {
                //TODO: logger
            }
        };

        this.scheduledExecutorService.scheduleWithFixedDelay(
                discoveryTask,
                0,
                config.getServiceDiscoveryDelay(),
                TimeUnit.MILLISECONDS
        );
    }

    @Override
    public void close() {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
    }
}
