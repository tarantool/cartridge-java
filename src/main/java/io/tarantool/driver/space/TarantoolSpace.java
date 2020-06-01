package io.tarantool.driver.space;

import io.tarantool.driver.TarantoolClient;
import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.TarantoolIndexQueryFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolSelectOptions;
import io.tarantool.driver.core.RequestManager;
import io.tarantool.driver.protocol.TarantoolIteratorType;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.requests.TarantoolSelectRequest;
import org.springframework.util.Assert;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Basic Tarantool space operations implementation for standalone server
 *
 * @author Alexey Kuzin
 */
public class TarantoolSpace implements TarantoolSpaceOperations {

    private int spaceId;
    private String name;
    private TarantoolClient client;
    private RequestManager requestManager;
    private TarantoolIndexQueryFactory indexQueryFactory;

    public TarantoolSpace(int spaceId, TarantoolClient client, RequestManager requestManager) {
        this.spaceId = spaceId;
        this.name = null;
        this.client = client;
        this.requestManager = requestManager;
        this.indexQueryFactory = new TarantoolIndexQueryFactory(client);
    }

    public TarantoolSpace(int spaceId, String name, TarantoolClient client, RequestManager requestManager) {
        this.spaceId = spaceId;
        this.name = name;
        this.client = client;
        this.requestManager = requestManager;
    }

    /**
     * Get space ID
     * @return space ID on the Tarantool server
     */
    public int getSpaceId() {
        return spaceId;
    }

    /**
     * Get name of the space
     * @return nullable name wrapped in {@code Optional}
     */
    public Optional<String> getName() {
        return Optional.ofNullable(name);
    }

    @Override
    public CompletableFuture<TarantoolResult> select(TarantoolSelectOptions options) throws TarantoolProtocolException {
        return select(indexQueryFactory.primary(), options);
    }

    @Override
    public CompletableFuture<TarantoolResult> select(String indexName, TarantoolSelectOptions options) throws TarantoolProtocolException {
        Assert.hasText(indexName, "Index name must not be null or empty");

        TarantoolIndexQuery indexQuery = indexQueryFactory.byName(indexName); // new TarantoolIndexQuery(client.metadata().getIndexForName(spaceId, indexName))
        return select(indexQuery, options);
    }

    @Override
    public CompletableFuture<TarantoolResult> select(String indexName, TarantoolIteratorType iteratorType, TarantoolSelectOptions options) throws TarantoolProtocolException {
        TarantoolIndexQuery indexQuery = indexQueryFactory.byName(indexName).withIteratorType(iteratorType);
        return select(indexQuery, options);
    }

    @Override
    public CompletableFuture<TarantoolResult> select(TarantoolIndexQuery indexQuery, TarantoolSelectOptions options) throws TarantoolProtocolException {
        TarantoolSelectRequest request = new TarantoolSelectRequest.Builder()
                .withSpaceId(spaceId)
                .withIndexId(indexQuery.getIndexId())
                .withIteratorType(indexQuery.getIteratorType())
                .withKeyValues(indexQuery.getKeyValues())
                .withLimit(options.getLimit())
                .withOffset(options.getOffset())
                .build(client.getConfig().getMapper());
        return requestManager.submitRequest(request);
    }

    @Override
    public CompletableFuture<TarantoolResult> update() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public CompletableFuture<TarantoolResult> replace() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public CompletableFuture<TarantoolResult> delete() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public String toString() {
        return String.format("TarantoolSpace [%d] (%s)", spaceId, (name != null ? name : ""));
    }
}
