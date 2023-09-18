package io.tarantool.driver.core.proxy.enums;

/**
 * Enum class representing a parts of arguments from which the final list of arguments is combined. The class was
 * introduced to combine interfaces with different numbers and types of compound arguments from which the final list
 * of arguments is combined.
 *
 * @author <a href="https://github.com/nickkkccc">Belonogov Nikolay</a>
 */
public enum ProxyOperationArgument {

    SPACE_NAME("space_name"),

    INDEX_QUERY("index_query"),

    TUPLE("tuple"),

    PROXY_QUERY("proxy_query"),

    TUPLE_OPERATIONS("tuple_operations"),

    OPTIONS("options");

    private final String name;

    ProxyOperationArgument(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return this.name;
    }
}
