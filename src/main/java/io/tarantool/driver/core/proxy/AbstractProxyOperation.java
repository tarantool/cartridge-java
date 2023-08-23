package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.space.options.Options;
import io.tarantool.driver.api.space.options.Self;
import io.tarantool.driver.core.proxy.enums.ProxyOperationArgument;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;

import java.util.Collection;
import java.util.EnumMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Basic implementation of a proxy operation
 *
 * @author Alexey Kuzin
 * @author Sergey Volgin
 * @author Artyom Dubinin
 */
abstract class AbstractProxyOperation<T> implements ProxyOperation<T> {

    protected final TarantoolCallOperations client;
    protected final String functionName;
    protected final Collection<?> arguments;
    private final Supplier<MessagePackObjectMapper> argumentsMapperSupplier;
    private final Supplier<CallResultMapper<T, SingleValueCallResult<T>>> resultMapperSupplier;

    AbstractProxyOperation(
        TarantoolCallOperations client,
        String functionName,
        Collection<?> arguments,
        Supplier<MessagePackObjectMapper> argumentsMapperSupplier,
        Supplier<CallResultMapper<T, SingleValueCallResult<T>>> resultMapperSupplier) {
        this.client = client;
        this.argumentsMapperSupplier = argumentsMapperSupplier;
        this.arguments = arguments;
        this.functionName = functionName;
        this.resultMapperSupplier = resultMapperSupplier;
    }

    public TarantoolCallOperations getClient() {
        return client;
    }

    public String getFunctionName() {
        return functionName;
    }

    public Collection<?> getArguments() {
        return arguments;
    }

    public Supplier<MessagePackObjectMapper> getArgumentsMapperSupplier() {
        return argumentsMapperSupplier;
    }

    public Supplier<CallResultMapper<T, SingleValueCallResult<T>>> getResultMapperSupplier() {
        return resultMapperSupplier;
    }

    @Override
    public CompletableFuture<T> execute() {
        return client.callForSingleResult(functionName, arguments, argumentsMapperSupplier, resultMapperSupplier);
    }

    abstract static
    class GenericOperationsBuilder<T, O extends Options, B extends GenericOperationsBuilder<T, O, B>>
        implements BuilderOptions, Self<B> {
        protected TarantoolCallOperations client;
        protected String functionName;
        protected EnumMap<ProxyOperationArgument, Object> arguments;
        protected Supplier<MessagePackObjectMapper> argumentsMapperSupplier;
        protected Supplier<CallResultMapper<T, SingleValueCallResult<T>>> resultMapperSupplier;

        GenericOperationsBuilder() {
            this.arguments = new EnumMap<>(ProxyOperationArgument.class);
        }

        public void addArgument(ProxyOperationArgument optionName, Object option) {
            this.arguments.put(optionName, option);
        }

        /**
         * Specify a client for sending and receiving requests from Tarantool server
         *
         * @param client Tarantool server client
         * @return builder
         */
        public B withClient(TarantoolCallOperations client) {
            this.client = client;
            return self();
        }

        /**
         * Specify name of Tarantool server space to work with
         *
         * @param spaceName name of Tarantool server space
         * @return builder
         */
        public B withSpaceName(String spaceName) {
            addArgument(ProxyOperationArgument.SPACE_NAME, spaceName);
            return self();
        }

        /**
         * Specify name of the Tarantool server function called through preparing request
         *
         * @param functionName name of Tarantool server function
         * @return builder
         */
        public B withFunctionName(String functionName) {
            this.functionName = functionName;
            return self();
        }

        /**
         * Specify entity-to-MessagePack mapper for arguments contents conversion
         *
         * @param argumentsMapperSupplier mapper supplier for arguments entity-to-MessagePack entity conversion
         * @return builder
         */
        public B withArgumentsMapperSupplier(Supplier<MessagePackObjectMapper> argumentsMapperSupplier) {
            this.argumentsMapperSupplier = argumentsMapperSupplier;
            return self();
        }

        /**
         * Specify MessagePack-to-entity mapper for result contents conversion
         *
         * @param resultMapperSupplier mapper supplier for result value MessagePack entity-to-object conversion
         * @return builder
         */
        public B withResultMapperSupplier(
            Supplier<CallResultMapper<T, SingleValueCallResult<T>>> resultMapperSupplier) {
            this.resultMapperSupplier = resultMapperSupplier;
            return self();
        }

        /**
         * Specify custom options
         *
         * @param options cluster proxy operation options
         * @return builder
         */
        public B withOptions(O options) {
            addArgument(ProxyOperationArgument.OPTIONS, options.asMap());
            return self();
        }
    }
}
