package io.tarantool.driver.cluster;

import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.core.TarantoolDaemonThreadFactory;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.exceptions.TarantoolClientException;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class offering discovery task creation and updating of the internal collection of addresses
 *
 * @author Alexey Kuzin
 */
public abstract class AbstractDiscoveryClusterAddressProvider implements TarantoolClusterAddressProvider {

    private final ClusterDiscoveryConfig discoveryConfig;
    private final ScheduledExecutorService scheduledExecutorService;
    private final CountDownLatch initLatch = new CountDownLatch(1);
    private final AtomicReference<Collection<TarantoolServerAddress>> addressesHolder = new AtomicReference<>();

    public AbstractDiscoveryClusterAddressProvider(ClusterDiscoveryConfig discoveryConfig) {
        this.discoveryConfig = discoveryConfig;
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                new TarantoolDaemonThreadFactory("tarantool-discovery"));
    }

    protected void startDiscoveryTask() throws TarantoolClientException {
        Runnable discoveryTask = () -> {
            Collection<TarantoolServerAddress> addresses = discoverAddresses();
            setAddresses(addresses);
            if (initLatch.getCount() > 0) {
                initLatch.countDown();
            }
        };

        this.scheduledExecutorService.scheduleWithFixedDelay(
                discoveryTask,
                0,
                discoveryConfig.getServiceDiscoveryDelay(),
                TimeUnit.MILLISECONDS
        );
    }

    protected ClusterDiscoveryConfig getDiscoveryConfig() {
        return discoveryConfig;
    }

    protected ScheduledExecutorService getExecutorService() {
        return scheduledExecutorService;
    }

    protected abstract Collection<TarantoolServerAddress> discoverAddresses();

    private void setAddresses(Collection<TarantoolServerAddress> addresses) {
        this.addressesHolder.set(addresses);
    }

    @Override
    public Collection<TarantoolServerAddress> getAddresses() {
        try {
            initLatch.await();
        } catch (InterruptedException e) {
            throw new TarantoolClientException("Interrupted while waiting for cluster addresses discovery");
        }
        return addressesHolder.get();
    }

    @Override
    public void close() {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
    }
}
