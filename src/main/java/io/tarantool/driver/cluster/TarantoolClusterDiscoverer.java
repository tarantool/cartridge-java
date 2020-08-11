package io.tarantool.driver.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tarantool.driver.ServerAddress;
import io.tarantool.driver.TarantoolClient;
import io.tarantool.driver.TarantoolConnection;
import io.tarantool.driver.exceptions.TarantoolClientException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Tarantool service discovery client.
 * Gets list of cluster node addresses calling stored Lua function.
 *
 * <p>
 * Expected response format:
 * <code>
 *127.0.0.1:3301> get_replica_set()
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
 * </p>
 * <p>Lua function example:
 * <code>
 * ...
 *	local function get_replica_set()
 *     local vshard = require('vshard')
 *     local router_info, err = vshard.router.info()
 *     if err ~= nil then
 *         error(err)
 *     end
 *
 *     local result = {}
 *     for i, v in pairs(router_info['replicasets']) do
 *         result[i] = v['master']
 *     end
 *     return result
 * end
 * ...
 * </code>
 * </p>
 *
 * @author Sergey Volgin
 */
public class TarantoolClusterDiscoverer implements ClusterDiscoverer {

    private final TarantoolClient client;
    private final String entryFunction;
    private final ObjectMapper objectMapper;

    public TarantoolClusterDiscoverer(TarantoolClient client, TarantoolClusterDiscoveryEndpoint endpoint) {
        this.client = client;
        this.entryFunction = endpoint.getEntryFunction();
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public List<ServerAddress> getNodes() {
        try {
            TarantoolConnection connection = client.connect();

            List<Object> functionResult = connection.call(entryFunction, Collections.emptyList()).get();

            String valueAsString = objectMapper.writeValueAsString(functionResult.get(0));

            TypeReference<HashMap<String, ServerNodeInfo>> typeReference =
                    new TypeReference<HashMap<String, ServerNodeInfo>>() {};

            Map<String, ServerNodeInfo> responseMap;
            try {
                responseMap = objectMapper.readValue(valueAsString, typeReference);
            } catch (Exception ignored) {
                throw new TarantoolClientException("Invalid result format (%s)", valueAsString);
            }

            return responseMap.values().stream()
                    .filter(v -> v.getStatus().equals("available"))
                    .map(v -> new ServerAddress(v.getUri())).collect(Collectors.toList());
        } catch (Exception ignored) {
            //TODO: logger
        }

        return null;
    }

    @Override
    public void close() {

    }
}
