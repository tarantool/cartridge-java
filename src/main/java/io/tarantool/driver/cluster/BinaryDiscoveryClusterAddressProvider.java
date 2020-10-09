package io.tarantool.driver.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tarantool.driver.StandaloneTarantoolClient;
import io.tarantool.driver.api.TarantoolClient;
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
 * <p>
 * Expected response format:
 * <pre>
 * <code>
 * 127.0.0.1:3301&gt; get_routers()
 * ---
 * - 36a1a75e-60f0-4400-8bdc-d93e2c5ca54b:
 *     priority: 1
 *     status: healthy
 *     uri: localhost:3301
 *     uuid: 9a3426db-f8f6-4e9f-ac80-e263527a59bc
 *   4141912c-34b8-4e40-a17e-7a6d80345954:
 *     priority: 1
 *     status: healthy
 *     uri: localhost:3311
 *     uuid: 898b4d01-4261-4006-85ea-a3500163cda0
 * ...
 * </code>
 * </pre>
 * <p>
 * Lua function example:
 * <pre>
 * <code>
 *  ...
 *  local function get_routers()
 *    local cartridge = require('cartridge')
 *    local function table_contains(table, element)
 *      for _, value in pairs(table) do
 *        if value == element then
 *          return true
 *        end
 *      end
 *      return false
 *    end
 *
 *    local servers, err = cartridge.admin_get_servers()
 *    local routers = {}
 *
 *    for _, server in pairs(servers) do
 *      if server.replicaset ~= nil then
 *        if table_contains(server.replicaset.roles, 'app.roles.custom') then
 *          routers[server.uuid] = {
 *              status = server.healthy,
 *              uuid = server.uuid,
 *              uri = server.uri,
 *              priority = server.priority
 *          }
 *        end
 *      end
 *    end
 *
 *    return routers
 *  end
 *  ...
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
                    .filter(ServerNodeInfo::isAvailable)
                    .map(v -> new TarantoolServerAddress(v.getUri()))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new TarantoolClientException("Cluster discovery task error", e);
        }
    }
}
