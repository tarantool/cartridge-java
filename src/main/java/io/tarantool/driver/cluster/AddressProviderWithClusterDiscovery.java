package io.tarantool.driver.cluster;

import io.tarantool.driver.TarantoolClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.exceptions.TarantoolClientException;

/**
 * Address provider supports {@link ClusterDiscoverer} functional
 *
 * @author Sergey Volgin
 */
public interface AddressProviderWithClusterDiscovery {

    void runClusterDiscovery(TarantoolClientConfig config, TarantoolClient tarantoolClient) throws TarantoolClientException, TarantoolClientException, TarantoolClientException;
}
