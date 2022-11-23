package io.tarantool.driver.api.proxy;

/**
 * Provides methods for function names in a Tarantool instance or a Tarantool Cartridge role for CRUD operations.
 *
 * @author Sergey Volgin
 * @author Alexey Kuzin
 */
public final class ProxyOperationsMappingConfig {

    public static final String CRUD_PREFIX = "crud.";
    public static final String SCHEMA_FUNCTION = "ddl.get_schema";
    public static final String DELETE_FUNCTION = CRUD_PREFIX + "delete";
    public static final String INSERT_FUNCTION = CRUD_PREFIX + "insert";
    public static final String INSERT_MANY_FUNCTION = CRUD_PREFIX + "insert_many";
    public static final String REPLACE_FUNCTION = CRUD_PREFIX + "replace";
    public static final String REPLACE_MANY_FUNCTION = CRUD_PREFIX + "replace_many";
    public static final String SELECT_FUNCTION = CRUD_PREFIX + "select";
    public static final String UPDATE_FUNCTION = CRUD_PREFIX + "update";
    public static final String UPSERT_FUNCTION = CRUD_PREFIX + "upsert";
    public static final String TRUNCATE_FUNCTION = CRUD_PREFIX + "truncate";

    private final String schemaFunctionName;
    private final String deleteFunctionName;
    private final String insertFunctionName;
    private final String insertManyFunctionName;
    private final String replaceFunctionName;
    private final String replaceManyFunctionName;
    private final String updateFunctionName;
    private final String upsertFunctionName;
    private final String selectFunctionName;
    private final String truncateFunctionName;

    /**
     * Get API function name for getting the spaces and indexes schema. The default value is
     * <code>ddl.get_schema</code>.
     * <p>
     * See <a href="https://github.com/tarantool/ddl/blob/eaa24b8931b3abba850e37a287091b67512e5d6c/ddl.lua#L127">
     * the DDL module API</a> for details on the desired schema metadata format.
     *
     * @return a callable API function name
     */
    public String getGetSchemaFunctionName() {
        return schemaFunctionName;
    }

    /**
     * Get API function name for performing the delete operation. The default value is <code>crud.delete</code>.
     *
     * @return a callable API function name
     */
    public String getDeleteFunctionName() {
        return deleteFunctionName;
    }

    /**
     * Get API function name for performing the insert operation. The default value is <code>crud.insert</code>.
     *
     * @return a callable API function name
     */
    public String getInsertFunctionName() {
        return insertFunctionName;
    }

    /**
     * Get API function name for performing the insert_many operation.
     * The default value is <code>crud.insert_many</code>.
     *
     * @return a callable API function name
     */
    public String getInsertManyFunctionName() {
        return insertManyFunctionName;
    }

    /**
     * Get API function name for performing the replace operation. The default value is <code>crud.replace</code>.
     *
     * @return a callable API function name
     */
    public String getReplaceFunctionName() {
        return replaceFunctionName;
    }

    /**
     * Get API function name for performing the replace_many operation.
     * The default value is <code>crud.replace_many</code>.
     *
     * @return a callable API function name
     */
    public String getReplaceManyFunctionName() {
        return replaceManyFunctionName;
    }

    /**
     * Get API function name for performing the update operation. The default value is <code>crud.update</code>.
     *
     * @return a callable API function name
     */
    public String getUpdateFunctionName() {
        return updateFunctionName;
    }

    /**
     * Get API function name for performing the upsert operation. The default value is <code>crud.upsert</code>.
     *
     * @return a callable API function name
     */
    public String getUpsertFunctionName() {
        return upsertFunctionName;
    }

    /**
     * Get API function name for performing the select operation. The default value is <code>crud.select</code>.
     *
     * @return a callable API function name
     */
    public String getSelectFunctionName() {
        return selectFunctionName;
    }

    /**
     * Get API function name for performing the select operation. The default value is <code>crud.truncate</code>.
     *
     * @return a callable API function name
     */
    public String getTruncateFunctionName() {
        return truncateFunctionName;
    }

    private ProxyOperationsMappingConfig(
        String schemaFunctionName, String deleteFunctionName,
        String insertFunctionName, String insertManyFunctionName,
        String replaceFunctionName, String replaceManyFunctionName,
        String updateFunctionName, String upsertFunctionName,
        String selectFunctionName, String truncateFunctionName) {
        this.schemaFunctionName = schemaFunctionName;
        this.deleteFunctionName = deleteFunctionName;
        this.insertFunctionName = insertFunctionName;
        this.insertManyFunctionName = insertManyFunctionName;
        this.replaceFunctionName = replaceFunctionName;
        this.replaceManyFunctionName = replaceManyFunctionName;
        this.updateFunctionName = updateFunctionName;
        this.upsertFunctionName = upsertFunctionName;
        this.selectFunctionName = selectFunctionName;
        this.truncateFunctionName = truncateFunctionName;
    }

    /**
     * Create new {@link Builder} instance
     *
     * @return new config builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for the {@link ProxyOperationsMappingConfig}
     */
    public static final class Builder {

        private String schemaFunctionName = SCHEMA_FUNCTION;
        private String deleteFunctionName = DELETE_FUNCTION;
        private String insertFunctionName = INSERT_FUNCTION;
        private String insertManyFunctionName = INSERT_MANY_FUNCTION;
        private String replaceFunctionName = REPLACE_FUNCTION;
        private String replaceManyFunctionName = REPLACE_MANY_FUNCTION;
        private String updateFunctionName = UPDATE_FUNCTION;
        private String upsertFunctionName = UPSERT_FUNCTION;
        private String selectFunctionName = SELECT_FUNCTION;
        private String truncateFunctionName = TRUNCATE_FUNCTION;

        /**
         * Set API function name for getting the spaces and indexes schema.
         * <p>
         * See <a href="https://github.com/tarantool/ddl/blob/eaa24b8931b3abba850e37a287091b67512e5d6c/ddl.lua#L127">
         * the DDL module API</a> for details on the desired schema metadata format.
         *
         * @param schemaFunctionName name for stored function returning spaces and indexes schema
         * @return a callable API function name
         */
        public Builder withSchemaFunctionName(String schemaFunctionName) {
            this.schemaFunctionName = schemaFunctionName;
            return this;
        }

        /**
         * Set API function name for performing the delete operation
         *
         * @param deleteFunctionName name for stored function performing delete operation
         * @return a callable API function name
         */
        public Builder withDeleteFunctionName(String deleteFunctionName) {
            this.deleteFunctionName = deleteFunctionName;
            return this;
        }

        /**
         * Get API function name for performing the insert operation
         *
         * @param insertFunctionName name for stored function performing insert operation
         * @return a callable API function name
         */
        public Builder withInsertFunctionName(String insertFunctionName) {
            this.insertFunctionName = insertFunctionName;
            return this;
        }

        /**
         * Get API function name for performing the insert_many operation
         *
         * @param insertManyFunctionName name for stored function performing insert_many operation
         * @return a callable API function name
         */
        public Builder withInsertManyFunctionName(String insertManyFunctionName) {
            this.insertManyFunctionName = insertManyFunctionName;
            return this;
        }

        /**
         * Get API function name for performing the replace operation
         *
         * @param replaceFunctionName name for stored function performing replace operation
         * @return a callable API function name
         */
        public Builder withReplaceFunctionName(String replaceFunctionName) {
            this.replaceFunctionName = replaceFunctionName;
            return this;
        }

        /**
         * Get API function name for performing the replace_many operation
         *
         * @param replaceManyFunctionName name for stored function performing replace_many operation
         * @return a callable API function name
         */
        public Builder withReplaceManyFunctionName(String replaceManyFunctionName) {
            this.replaceManyFunctionName = replaceManyFunctionName;
            return this;
        }

        /**
         * Get API function name for performing the update operation
         *
         * @param updateFunctionName name for stored function performing update operation
         * @return a callable API function name
         */
        public Builder withUpdateFunctionName(String updateFunctionName) {
            this.updateFunctionName = updateFunctionName;
            return this;
        }

        /**
         * Get API function name for performing the upsert operation
         *
         * @param upsertFunctionName name for stored function performing upsert operation
         * @return a callable API function name
         */
        public Builder withUpsertFunctionName(String upsertFunctionName) {
            this.upsertFunctionName = upsertFunctionName;
            return this;
        }

        /**
         * Get API function name for performing the select operation
         *
         * @param selectFunctionName name for stored function performing select operation
         * @return a callable API function name
         */
        public Builder withSelectFunctionName(String selectFunctionName) {
            this.selectFunctionName = selectFunctionName;
            return this;
        }

        /**
         * Get API function name for performing the truncate operation
         *
         * @param truncateFunctionName name for stored function performing select operation
         * @return a callable API function name
         */
        public Builder withTruncateFunctionName(String truncateFunctionName) {
            this.truncateFunctionName = truncateFunctionName;
            return this;
        }

        /**
         * Build a new {@link ProxyOperationsMappingConfig} instance
         *
         * @return new config instance
         */
        public ProxyOperationsMappingConfig build() {
            return new ProxyOperationsMappingConfig(schemaFunctionName, deleteFunctionName, insertFunctionName,
                insertManyFunctionName, replaceFunctionName, replaceManyFunctionName, updateFunctionName,
                upsertFunctionName, selectFunctionName, truncateFunctionName);
        }
    }
}
