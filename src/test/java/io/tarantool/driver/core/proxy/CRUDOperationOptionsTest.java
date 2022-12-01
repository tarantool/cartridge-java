package io.tarantool.driver.core.proxy;

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

        assertEquals(1000, options.asMap().get(CRUDBaseOptions.TIMEOUT));
        assertEquals(50L, options.asMap().get(CRUDSelectOptions.SELECT_LIMIT));
        assertEquals(10, options.asMap().get(CRUDSelectOptions.SELECT_BATCH_SIZE));
        assertEquals(tuple, options.asMap().get(CRUDSelectOptions.SELECT_AFTER));
    }

    @Test
    public void baseOperationOptions_createNotEmptyTest() {
        CRUDBaseOptions options = new CRUDBaseOptions.Builder()
            .withTimeout(Optional.of(1000))
            .build();

        assertEquals(1, options.asMap().size());
        assertEquals(1000, options.asMap().get(CRUDBaseOptions.TIMEOUT));
    }

    @Test
    public void batchOperationOptions_createNotEmptyTest() {
        CRUDBatchOptions options = new CRUDBatchOptions.Builder()
            .withStopOnError(Optional.of(false))
            .withRollbackOnError(Optional.of(true))
            .build();

        assertEquals(2, options.asMap().size());
        assertEquals(false, options.asMap().get(CRUDBatchOptions.BATCH_STOP_ON_ERROR));
        assertEquals(true, options.asMap().get(CRUDBatchOptions.BATCH_ROLLBACK_ON_ERROR));
    }
}
