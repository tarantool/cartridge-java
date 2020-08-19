package io.tarantool.driver.cluster;

import io.tarantool.driver.TarantoolServerAddress;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Basic strategy that changes addresses in a round-robin fashion.
 *
 * @author Sergey Volgin
 */
public class RoundRobinAddressProvider implements AddressProvider, SimpleAddressProvider {

    private final List<TarantoolServerAddress> addressList = new ArrayList<>();
    private AtomicInteger currentPosition = new AtomicInteger(-1);

    /**
     * Constructs an instance.
     *
     * @param addresses optional array of addresses in a form of host[:port]
     * @throws IllegalArgumentException if addresses aren't provided
     */
    public RoundRobinAddressProvider(List<TarantoolServerAddress> addresses) {
        updateAddressList(addresses);
    }

    @Override
    public TarantoolServerAddress getCurrentAddress() {
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
    public TarantoolServerAddress getNextAddress() {
        synchronized (addressList) {
            int position = currentPosition.updateAndGet(i -> (i + 1) % addressList.size());
            return addressList.get(position);
        }
    }

    /**
     * Update provider address list. A duplicate server addresses are removed from the list.
     *
     * @param addresses list {@link TarantoolServerAddress} of cluster nodes
     */
    @Override
    public void updateAddressList(Collection<TarantoolServerAddress> addresses) {
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalArgumentException("At least one server address must be provided");
        }
        synchronized (addressList) {
            TarantoolServerAddress currentAddress = getCurrentAddress();
            Set<TarantoolServerAddress> hostsSet = new LinkedHashSet<>(addresses.size());
            addressList.clear();

            for (TarantoolServerAddress tarantoolServerAddress : addresses) {
                Assert.notNull(tarantoolServerAddress, "TarantoolServerAddress must not be null");
                hostsSet.add(new TarantoolServerAddress(tarantoolServerAddress.getHost(),
                        tarantoolServerAddress.getPort()));
            }

            addressList.addAll(hostsSet);

            int newPosition = addressList.indexOf(currentAddress);
            currentPosition.set(newPosition);
        }
    }

    /**
     * Get number of {@link TarantoolServerAddress} in the pool
     *
     * @return number of {@link TarantoolServerAddress} in the pool
     */
    public int size() {
        synchronized (addressList) {
            return addressList.size();
        }
    }
}
