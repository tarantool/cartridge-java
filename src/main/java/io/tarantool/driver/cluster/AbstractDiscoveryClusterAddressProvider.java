package io.tarantool.driver.cluster;

import io.tarantool.driver.api.TarantoolClusterAddressProvider;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.core.TarantoolDaemonThreadFactory;
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

    private final TarantoolClusterDiscoveryConfig discoveryConfig;
    private final ScheduledExecutorService scheduledExecutorService;
    private final CountDownLatch initLatch = new CountDownLatch(1);
    private final AtomicReference<Collection<TarantoolServerAddress>> addressesHolder = new AtomicReference<>();
    private final AtomicReference<Runnable> refreshCallback;

    public AbstractDiscoveryClusterAddressProvider(TarantoolClusterDiscoveryConfig discoveryConfig) {
        this.discoveryConfig = discoveryConfig;
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
            new TarantoolDaemonThreadFactory("tarantool-discovery"));
        this.refreshCallback = new AtomicReference<>(() -> {
        });
    }

    protected void startDiscoveryTask() throws TarantoolClientException {
        Runnable discoveryTask = () -> {
            try {
                setAddresses(discoverAddresses());
            } finally {
                if (initLatch.getCount() > 0) {
                    initLatch.countDown();
                }
            }
            this.refreshCallback.get().run();
        };

        this.scheduledExecutorService.scheduleWithFixedDelay(
            discoveryTask,
            0,
            discoveryConfig.getServiceDiscoveryDelay(),
            TimeUnit.MILLISECONDS
        );
    }

    protected TarantoolClusterDiscoveryConfig getDiscoveryConfig() {
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
    public void setRefreshCallback(Runnable runnable) {
        this.refreshCallback.set(runnable);
    }

    @Override
    public void close() {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
    }
}
