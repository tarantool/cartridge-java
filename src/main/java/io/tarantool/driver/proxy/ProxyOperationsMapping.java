package io.tarantool.driver.proxy;

/**
 * Provides methods for function names in a Tarantool instance or a Tarantool Cartridge role for CRUD operations.
 *
 * @author Sergey Volgin
 */
public interface ProxyOperationsMapping {

    String FUNCTION_PREFIX = "crud";
    String SCHEMA_FUNCTION = FUNCTION_PREFIX + "_get_schema";
    String DELETE_FUNCTION = FUNCTION_PREFIX + "_delete";
    String INSERT_FUNCTION = FUNCTION_PREFIX + "_insert";
    String REPLACE_FUNCTION = FUNCTION_PREFIX + "_replace";
    String SELECT_FUNCTION = FUNCTION_PREFIX + "_select";
    String UPDATE_FUNCTION = FUNCTION_PREFIX + "_update";
    String UPSERT_UPSERT = FUNCTION_PREFIX + "_upsert";

    /**
     * Get API function name for getting the spaces and indexes schema. The default value is
     * <code>crud_get_schama</code>.
     *
     * See <a href="https://github.com/tarantool/ddl/blob/eaa24b8931b3abba850e37a287091b67512e5d6c/ddl.lua#L127">
     * the DDL module API</a> for details on the desired schema metadata format.
     *
     * @return a callable API function name
     */
    default String getGetSchemaFunctionName() {
        return SCHEMA_FUNCTION;
    }

    /**
     * Get API function name for performing the delete operation. The default value is <code>crud_delete</code>.
     *
     * @return a callable API function name
     */
    default String getDeleteFunctionName() {
        return DELETE_FUNCTION;
    }

    /**
     * Get API function name for performing the insert operation. The default value is <code>crud_insert</code>.
     *
     * @return a callable API function name
     */
    default String getInsertFunctionName() {
        return INSERT_FUNCTION;
    }

    /**
     * Get API function name for performing the replace operation. The default value is <code>crud_replace</code>.
     *
     * @return a callable API function name
     */
    default String getReplaceFunctionName() {
        return REPLACE_FUNCTION;
    }

    /**
     * Get API function name for performing the select operation. The default value is <code>crud_select</code>.
     *
     * @return a callable API function name
     */
    default String getSelectFunctionName() {
        return SELECT_FUNCTION;
    }

    /**
     * Get API function name for performing the update operation. The default value is <code>crud_update</code>.
     *
     * @return a callable API function name
     */
    default String getUpdateFunctionName() {
        return UPDATE_FUNCTION;
    }

    /**
     * Get API function name for performing the upsert operation. The default value is <code>crud_upsert</code>.
     *
     * @return a callable API function name
     */
    default String getUpsertFunctionName() {
        return UPSERT_UPSERT;
    }
}
