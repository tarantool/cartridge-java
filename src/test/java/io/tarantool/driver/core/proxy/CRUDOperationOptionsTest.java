package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.space.options.enums.crud.ProxyOption;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.core.tuple.TarantoolTupleImpl;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Sergey Volgin
 */
public class CRUDOperationOptionsTest {

    @Test
    public void selectOperationOptions_createEmptyTest() {
        CRUDSelectOptions options = new CRUDSelectOptions.Builder().build();
        assertEquals(Collections.EMPTY_MAP, options.asMap());
    }

    @Test
    public void selectOperationOptions_createNotEmptyTest() {
        MessagePackMapper defaultMapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
        List<Object> values = Arrays.asList(4, "a4", "Nineteen Eighty-Four", "George Orwell", 1984);
        TarantoolTuple tuple = new TarantoolTupleImpl(values, defaultMapper);

        CRUDSelectOptions options = new CRUDSelectOptions.Builder()
            .withTimeout(Optional.of(1000))
            .withSelectLimit(Optional.of(50L))
            .withSelectBatchSize(Optional.of(10))
            .withSelectAfter(Optional.of(tuple))
            .build();

        assertEquals(4, options.asMap().size());

        assertEquals(1000, options.asMap().get(ProxyOption.TIMEOUT.toString()));
        assertEquals(50L, options.asMap().get(ProxyOption.FIRST.toString()));
        assertEquals(10, options.asMap().get(ProxyOption.BATCH_SIZE.toString()));
        assertEquals(tuple, options.asMap().get(ProxyOption.AFTER.toString()));
    }

    @Test
    public void baseOperationOptions_createNotEmptyTest() {
        CRUDBaseOptions options = new CRUDBaseOptions.Builder()
            .withTimeout(Optional.of(1000))
            .build();

        assertEquals(1, options.asMap().size());
        assertEquals(1000, options.asMap().get(ProxyOption.TIMEOUT.toString()));
    }

    @Test
    public void batchOperationOptions_createNotEmptyTest() {
        CRUDBatchOptions options = new CRUDBatchOptions.Builder()
            .withStopOnError(Optional.of(false))
            .withRollbackOnError(Optional.of(true))
            .build();

        assertEquals(2, options.asMap().size());
        assertEquals(false, options.asMap().get(ProxyOption.STOP_ON_ERROR.toString()));
        assertEquals(true, options.asMap().get(ProxyOption.ROLLBACK_ON_ERROR.toString()));
    }
}
