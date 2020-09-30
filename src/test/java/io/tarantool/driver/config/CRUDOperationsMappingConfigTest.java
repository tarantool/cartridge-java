package io.tarantool.driver.config;

import io.tarantool.driver.proxy.CRUDOperationsMappingConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Sergey Volgin
 */
public class CRUDOperationsMappingConfigTest {

    @Test
    public void createClusterProxySpaceConfigDefault() {
        CRUDOperationsMappingConfig mapping = new CRUDOperationsMappingConfig("prefix", "get_cluster_schema");

        assertEquals(mapping.getGetSchemaFunctionName(), "get_cluster_schema");
        assertEquals(mapping.getDeleteFunctionName(), "prefix_delete");
        assertEquals(mapping.getInsertFunctionName(), "prefix_insert");
        assertEquals(mapping.getReplaceFunctionName(), "prefix_replace");
        assertEquals(mapping.getSelectFunctionName(), "prefix_select");
        assertEquals(mapping.getUpdateFunctionName(), "prefix_update");
        assertEquals(mapping.getUpsertFunctionName(), "prefix_upsert");
    }

    @Test
    public void createClusterProxySpaceConfigBuilder() {
        CRUDOperationsMappingConfig mapping = new CRUDOperationsMappingConfig.Builder()
                .withGetSchemaFunctionName("func")
                .withDeleteFunctionName("func1")
                .withInsertFunctionName("func2")
                .withReplaceFunctionName("func3")
                .withSelectFunctionName("func4")
                .withUpdateFunctionName("func5")
                .withUpsertFunctionName("func6")
                .build();

        assertEquals(mapping.getGetSchemaFunctionName(), "func");
        assertEquals(mapping.getDeleteFunctionName(), "func1");
        assertEquals(mapping.getInsertFunctionName(), "func2");
        assertEquals(mapping.getReplaceFunctionName(), "func3");
        assertEquals(mapping.getSelectFunctionName(), "func4");
        assertEquals(mapping.getUpdateFunctionName(), "func5");
        assertEquals(mapping.getUpsertFunctionName(), "func6");
    }

    @Test
    public void invalidConfig() {
        assertThrows(IllegalArgumentException.class, () -> new CRUDOperationsMappingConfig.Builder()
                .withGetSchemaFunctionName("func")
                .withDeleteFunctionName("func1")
                .withInsertFunctionName("func2")
                .withReplaceFunctionName("func3")
                .withSelectFunctionName("func4")
                .withUpdateFunctionName("func5")
                .withUpsertFunctionName("")
                .build());

        assertThrows(IllegalArgumentException.class, () -> CRUDOperationsMappingConfig.builder().build());
    }
}
