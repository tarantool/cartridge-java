package io.tarantool.driver.metadata;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolMetadataRequestException;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.ValueConverter;
import org.msgpack.value.Value;

import java.util.concurrent.CompletableFuture;

/**
 * Provides spaces and index metadata via stored function call
 *
 * @author Alexey Kuzin
 */
public class ProxyMetadataProvider implements TarantoolMetadataProvider {

    private final String metadataFunctionName;
    private final TarantoolCallOperations client;
    private final ValueConverter<Value, TarantoolMetadataContainer> metadataConverter;
    private final Class<? extends SingleValueCallResult<TarantoolMetadataContainer>> resultClass;

    /**
     * Basic constructor
     *
     * @param client configured Tarantool client to make requests with
     * @param metadataFunctionName the stored function name
     * @param metadataConverter converter to {@link TarantoolMetadataContainer} aware of the function call result format
     * @param resultClass result class
     */
    public ProxyMetadataProvider(TarantoolCallOperations client,
                                 String metadataFunctionName,
                                 ValueConverter<Value, TarantoolMetadataContainer> metadataConverter,
                                 Class<? extends SingleValueCallResult<TarantoolMetadataContainer>> resultClass) {
        this.metadataFunctionName = metadataFunctionName;
        this.client = client;
        this.metadataConverter = metadataConverter;
        this.resultClass = resultClass;
    }

    @Override
    public CompletableFuture<TarantoolMetadataContainer> getMetadata() {
        CallResultMapper<TarantoolMetadataContainer, SingleValueCallResult<TarantoolMetadataContainer>> mapper =
                client.getResultMapperFactoryFactory().singleValueResultMapperFactory(TarantoolMetadataContainer.class)
                        .withSingleValueResultConverter(metadataConverter, resultClass);
        return client.callForSingleResult(metadataFunctionName, mapper)
            .exceptionally(ex -> {
                if (ex.getCause() != null && ex.getCause() instanceof TarantoolClientException) {
                    throw new TarantoolMetadataRequestException(metadataFunctionName, ex);
                } else if (ex instanceof RuntimeException) {
                    throw (RuntimeException) ex;
                } else {
                    throw new RuntimeException(ex);
                }
            });
    }
}
