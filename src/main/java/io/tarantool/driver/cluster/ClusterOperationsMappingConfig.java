package io.tarantool.driver.cluster;

import org.springframework.util.StringUtils;

/**
 * Configuring function names in cartridge role for CRUD operations on the cluster via elect.rock API
 * and name of function for get space metadata.
 *
 * //TODO: add link for elect.rock public repo
 *
 * @author Sergey Volgin
 */
public final class ClusterOperationsMappingConfig {

    private final String getSchemaFunctionName;
    private final String deleteFunctionName;
    private final String insertFunctionName;
    private final String replaceFunctionName;
    private final String selectFunctionName;
    private final String updateFunctionName;
    private final String upsertFunctionName;

    /**
     * Creates {@link ClusterOperationsMappingConfig} with default suffixes
     *
     * @param functionPrefix CRUD functions name prefix
     * @param getSchemaFunctionName the function name for obtain cluster space schema
     */
    public ClusterOperationsMappingConfig(String functionPrefix, String getSchemaFunctionName) {
        if (StringUtils.isEmpty(functionPrefix)) {
            throw new IllegalArgumentException("Function prefix must be not empty");
        }
        if (StringUtils.isEmpty(getSchemaFunctionName)) {
            throw new IllegalArgumentException("getSchemaFunctionName must be not empty");
        }
        this.getSchemaFunctionName = getSchemaFunctionName;
        this.deleteFunctionName = functionPrefix + "_delete";
        this.insertFunctionName = functionPrefix + "_insert";
        this.replaceFunctionName = functionPrefix + "_replace";
        this.selectFunctionName = functionPrefix + "_select";
        this.updateFunctionName = functionPrefix + "_update";
        this.upsertFunctionName = functionPrefix + "_upsert";
    }

    /**
     * Get a builder for this class.
     *
     * @return a new Builder for creating {@link ClusterOperationsMappingConfig}.
     */
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String getSchemaFunctionName;
        private String deleteFunctionName;
        private String insertFunctionName;
        private String replaceFunctionName;
        private String selectFunctionName;
        private String updateFunctionName;
        private String upsertFunctionName;

        public Builder() {
        }

        public Builder withGetSchemaFunctionName(String getSchemaFunctionName) {
            this.getSchemaFunctionName = getSchemaFunctionName;
            return this;
        }

        public Builder withDeleteFunctionName(String deleteFunctionName) {
            this.deleteFunctionName = deleteFunctionName;
            return this;
        }

        public Builder withInsertFunctionName(String insertFunctionName) {
            this.insertFunctionName = insertFunctionName;
            return this;
        }

        public Builder withReplaceFunctionName(String replaceFunctionName) {
            this.replaceFunctionName = replaceFunctionName;
            return this;
        }

        public Builder withSelectFunctionName(String selectFunctionName) {
            this.selectFunctionName = selectFunctionName;
            return this;
        }

        public Builder withUpdateFunctionName(String updateFunctionName) {
            this.updateFunctionName = updateFunctionName;
            return this;
        }

        public Builder withUpsertFunctionName(String upsertFunctionName) {
            this.upsertFunctionName = upsertFunctionName;
            return this;
        }

        public ClusterOperationsMappingConfig build() {
            return new ClusterOperationsMappingConfig(this);
        }
    }

    public String getGetSchemaFunctionName() {
        return getSchemaFunctionName;
    }

    public String getDeleteFunctionName() {
        return deleteFunctionName;
    }

    public String getInsertFunctionName() {
        return insertFunctionName;
    }

    public String getReplaceFunctionName() {
        return replaceFunctionName;
    }

    public String getSelectFunctionName() {
        return selectFunctionName;
    }

    public String getUpdateFunctionName() {
        return updateFunctionName;
    }

    public String getUpsertFunctionName() {
        return upsertFunctionName;
    }

    private ClusterOperationsMappingConfig(Builder builder) {
        if (StringUtils.isEmpty(builder.getSchemaFunctionName)) {
            throw new IllegalArgumentException("Get schema function name must be not empty");
        }
        if (StringUtils.isEmpty(builder.deleteFunctionName)) {
            throw new IllegalArgumentException("Delete function name must be not empty");
        }
        if (StringUtils.isEmpty(builder.insertFunctionName)) {
            throw new IllegalArgumentException("Insert function name must be not empty");
        }
        if (StringUtils.isEmpty(builder.replaceFunctionName)) {
            throw new IllegalArgumentException("Replace function name must be not empty");
        }
        if (StringUtils.isEmpty(builder.updateFunctionName)) {
            throw new IllegalArgumentException("Update function name must be not empty");
        }
        if (StringUtils.isEmpty(builder.selectFunctionName)) {
            throw new IllegalArgumentException("Select function name must be not empty");
        }
        if (StringUtils.isEmpty(builder.upsertFunctionName)) {
            throw new IllegalArgumentException("Upsert function name must be not empty");
        }

        this.getSchemaFunctionName = builder.getSchemaFunctionName;
        this.deleteFunctionName = builder.deleteFunctionName;
        this.insertFunctionName = builder.insertFunctionName;
        this.replaceFunctionName = builder.replaceFunctionName;
        this.selectFunctionName = builder.selectFunctionName;
        this.updateFunctionName = builder.updateFunctionName;
        this.upsertFunctionName = builder.upsertFunctionName;
    }
}
