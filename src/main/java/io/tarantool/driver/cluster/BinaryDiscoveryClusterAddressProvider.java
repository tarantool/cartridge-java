package io.tarantool.driver.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tarantool.driver.StandaloneTarantoolClient;
import io.tarantool.driver.TarantoolClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.exceptions.TarantoolClientException;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Service discovery client connecting to Tarantool via the binary protocol.
 * Gets list of cluster node addresses calling an exposed Lua function.
 *
 * Expected response format:
 * <pre>
 * <code>
 * 127.0.0.1:3301&gt; get_replica_set()
 * ---
 * - 36a1a75e-60f0-4400-8bdc-d93e2c5ca54b:
 *     network_timeout: 0.5
 *     status: available
 *     uri: admin@localhost:3302
 *     uuid: 9a3426db-f8f6-4e9f-ac80-e263527a59bc
 *   4141912c-34b8-4e40-a17e-7a6d80345954:
 *     network_timeout: 0.5
 *     status: available
 *     uri: admin@localhost:3304
 *     uuid: 898b4d01-4261-4006-85ea-a3500163cda0
 * ...
 * </code>
 * </pre>
 *
 * Lua function example:
 * <pre>
 * <code>
 * ...
 * local function get_replica_set()
 *   local vshard = require('vshard')
 *   local router_info, err = vshard.router.info()
 *   if err ~= nil then
 *     error(err)
 *   end
 *
 *   local result = {}
 *   for i, v in pairs(router_info['replicasets']) do
 *     result[i] = v['master']
 *   end
 *   return result
 * end
 * ...
 * </code>
 * </pre>
 *
 * @author Sergey Volgin
 */
public class BinaryDiscoveryClusterAddressProvider extends AbstractDiscoveryClusterAddressProvider {

    private final BinaryClusterDiscoveryEndpoint endpoint;
    private final TarantoolClient client;
    private final ObjectMapper objectMapper;

    public BinaryDiscoveryClusterAddressProvider(TarantoolClusterDiscoveryConfig discoveryConfig) {
        super(discoveryConfig);
        this.endpoint = (BinaryClusterDiscoveryEndpoint) discoveryConfig.getEndpoint();

        TarantoolClientConfig config = TarantoolClientConfig.builder()
                .withCredentials(endpoint.getCredentials())
                .withConnectTimeout(discoveryConfig.getConnectTimeout())
                .withReadTimeout(discoveryConfig.getReadTimeout())
                .build();

        this.client = new StandaloneTarantoolClient(config, endpoint.getServerAddress());
        this.objectMapper = new ObjectMapper();
        startDiscoveryTask();
    }

    protected Collection<TarantoolServerAddress> discoverAddresses() {
        try {
            List<Object> functionResult = client.call(endpoint.getDiscoveryFunction(), Collections.emptyList()).get();
            String valueAsString = objectMapper.writeValueAsString(functionResult.get(0));
            TypeReference<HashMap<String, ServerNodeInfo>> typeReference =
                    new TypeReference<HashMap<String, ServerNodeInfo>>() {
                    };

            Map<String, ServerNodeInfo> responseMap;
            try {
                responseMap = objectMapper.readValue(valueAsString, typeReference);
            } catch (Exception ignored) {
                throw new TarantoolClientException("Invalid result format (%s)", valueAsString);
            }

            return responseMap.values().stream()
                    .filter(v -> v.getStatus().equals("available"))
                    .map(v -> new TarantoolServerAddress(v.getUri()))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new TarantoolClientException("Cluster discovery task error", e);
        }
    }
}
