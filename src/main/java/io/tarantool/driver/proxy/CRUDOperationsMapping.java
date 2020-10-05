package io.tarantool.driver.proxy;

/**
 * Methods for return function names in cartridge role for CRUD operations on the cluster via crud.rock API
 * and name of function for get space metadata.
 *
 * @author Sergey Volgin
 * @see <a href="https://github.com/tarantool/crud">https://github.com/tarantool/crud</a>
 */
public interface CRUDOperationsMapping {

    String FUNCTION_PREFIX = "crud";
    String SCHEMA_FUNCTION = FUNCTION_PREFIX + "_cluster_schema";
    String DELETE_FUNCTION = FUNCTION_PREFIX + "_delete";
    String INSERT_FUNCTION = FUNCTION_PREFIX + "_insert";
    String REPLACE_FUNCTION = FUNCTION_PREFIX + "_replace";
    String SELECT_FUNCTION = FUNCTION_PREFIX + "_select";
    String UPDATE_FUNCTION = FUNCTION_PREFIX + "_update";
    String UPSERT_UPSERT = FUNCTION_PREFIX + "_upsert";

    default String getGetSchemaFunctionName() {
        return SCHEMA_FUNCTION;
    }

    default String getDeleteFunctionName() {
        return DELETE_FUNCTION;
    }

    default String getInsertFunctionName() {
        return INSERT_FUNCTION;
    }

    default String getReplaceFunctionName() {
        return REPLACE_FUNCTION;
    }

    default String getSelectFunctionName() {
        return SELECT_FUNCTION;
    }

    default String getUpdateFunctionName() {
        return UPDATE_FUNCTION;
    }

    default String getUpsertFunctionName() {
        return UPSERT_UPSERT;
    }
}
