package io.tarantool.driver.config;

import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleImpl;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.proxy.CRUDOperationOptions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Sergey Volgin
 */
public class CRUDOperationOptionsTest {

    @Test
    public void createEmptyTest() {
        CRUDOperationOptions options = CRUDOperationOptions.builder().build();
        assertEquals(Collections.EMPTY_MAP, options.asMap());
    }

    @Test
    public void createNotEmptyTest() {
        MessagePackMapper defaultMapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
        List<Object> values = Arrays.asList(4, "a4", "Nineteen Eighty-Four", "George Orwell", 1984);
        TarantoolTuple tuple = new TarantoolTupleImpl(values, defaultMapper);

        CRUDOperationOptions options = CRUDOperationOptions.builder()
                .withTimeout(1000)
                .withTuplesToMap(true)
                .withSelectLimit(50)
                .withSelectBatchSize(10)
                .withSelectAfter(tuple)
                .build();

        assertEquals(5, options.asMap().size());

        assertEquals(1000, options.asMap().get(CRUDOperationOptions.TIME_OUT));
        assertTrue((Boolean) options.asMap().get(CRUDOperationOptions.TUPLES_TO_MAP));
        assertEquals(50L, options.asMap().get(CRUDOperationOptions.SELECT_LIMIT));
        assertEquals(10L, options.asMap().get(CRUDOperationOptions.SELECT_BATCH_SIZE));
        assertEquals(tuple, options.asMap().get(CRUDOperationOptions.SELECT_AFTER));
    }
}
