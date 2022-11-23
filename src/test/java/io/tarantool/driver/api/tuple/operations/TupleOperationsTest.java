package io.tarantool.driver.api.tuple.operations;


import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolField;
import io.tarantool.driver.api.tuple.TarantoolNullField;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.exceptions.TarantoolSpaceOperationException;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.Test;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TupleOperationsTest {

    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
    private static final TarantoolTupleFactory tupleFactory =
        new DefaultTarantoolTupleFactory(mapperFactory.defaultComplexTypesMapper());

    @Test
    public void createInvalidOperationsList() {
        //double update of the same field by field number
        assertThrows(TarantoolSpaceOperationException.class,
            () -> TupleOperations.add(1, 10).andSubtract(1, 5));
        //double update of the same field by field name
        assertThrows(TarantoolSpaceOperationException.class,
            () -> TupleOperations.bitwiseOr("field_name", 10).andBitwiseAnd("field_name", 5));
    }

    @Test
    public void createOperationsList() {
        TupleOperations operations = TupleOperations
            .bitwiseXor(1, 5)
            .andBitwiseXor("field_1", 44)
            .andBitwiseAnd(2, 15)
            .andBitwiseAnd("field_2", 45)
            .andBitwiseOr(3, 10)
            .andBitwiseOr("field_3", 45)
            .andBitwiseXor(8, 12)
            .andBitwiseXor("field_4", 128)
            .andAdd(10, 10)
            .andAdd("field_5", 10)
            .andSet(11, 11)
            .andSet("field_6", "asdf")
            .andSubtract(15, 100)
            .andSubtract("field_7", 99)
            .andInsert(20, "kjlkj")
            .andInsert("field_8", "nhshs");

        assertEquals(16, operations.asList().size());

        operations.andSplice(22, 5, 1, "ndnd");
        operations.andSplice("field_9", 6, 10, "ndnd");

        assertEquals(18, operations.asList().size());

        List<TarantoolUpdateOperationType> expectedOperationTypes = Arrays.asList(
            TarantoolUpdateOperationType.BITWISEXOR, TarantoolUpdateOperationType.BITWISEXOR,
            TarantoolUpdateOperationType.BITWISEAND, TarantoolUpdateOperationType.BITWISEAND,
            TarantoolUpdateOperationType.BITWISEOR, TarantoolUpdateOperationType.BITWISEOR,
            TarantoolUpdateOperationType.BITWISEXOR, TarantoolUpdateOperationType.BITWISEXOR,
            TarantoolUpdateOperationType.ADD, TarantoolUpdateOperationType.ADD,
            TarantoolUpdateOperationType.SET, TarantoolUpdateOperationType.SET,
            TarantoolUpdateOperationType.SUBTRACT, TarantoolUpdateOperationType.SUBTRACT,
            TarantoolUpdateOperationType.INSERT, TarantoolUpdateOperationType.INSERT,
            TarantoolUpdateOperationType.SPLICE, TarantoolUpdateOperationType.SPLICE);

        List<TarantoolUpdateOperationType> actualOperationTypes = operations.asList().stream()
            .map(TupleOperation::getOperationType).collect(Collectors.toList());

        assertEquals(expectedOperationTypes, actualOperationTypes);
    }

    @Test
    public void convertIndexToPositionNumberOperationsList() {
        TupleOperations operations = TupleOperations
            .bitwiseXor(8, 5)
            .andBitwiseAnd(2, 15);

        assertEquals(2, operations.asList().size());

        List<Integer> fieldIndexes = operations.asList().stream().map(TupleOperation::getFieldIndex)
            .collect(Collectors.toList());

        assertEquals(Arrays.asList(8, 2), fieldIndexes);

        List<Integer> fieldNumbers = operations.asProxyOperationList().stream().map(TupleOperation::getFieldIndex)
            .collect(Collectors.toList());

        assertEquals(Arrays.asList(9, 3), fieldNumbers);
    }

    @Test
    public void fromTarantoolTuple() {
        TupleOperations operations;

        assertThrows(RuntimeException.class, () -> TupleOperations.fromTarantoolTuple(null));
        assertThrows(TarantoolSpaceOperationException.class,
            () -> TupleOperations.fromTarantoolTuple(tupleFactory.create()));

        operations = TupleOperations.fromTarantoolTuple(tupleFactory.create("abc", "def"));
        List<Integer> fieldNumbers = operations.asList().stream().map(TupleOperation::getFieldIndex)
            .collect(Collectors.toList());
        assertEquals(Arrays.asList(0, 1), fieldNumbers);
        String value = ((TarantoolField) operations.asList().get(1).getValue())
            .getValue(String.class, mapperFactory.defaultComplexTypesMapper());
        assertEquals("def", value);

        operations = TupleOperations.fromTarantoolTuple(tupleFactory.create("abc", null, "def"));
        fieldNumbers = operations.asList().stream().map(TupleOperation::getFieldIndex)
            .collect(Collectors.toList());
        assertEquals(Arrays.asList(0, 1, 2), fieldNumbers);
        assertEquals(TarantoolNullField.INSTANCE, operations.asList().get(1).getValue());

        operations = TupleOperations.fromTarantoolTuple(tupleFactory.create("abc", null, null));
        fieldNumbers = operations.asList().stream().map(TupleOperation::getFieldIndex)
            .collect(Collectors.toList());
        assertEquals(Arrays.asList(0, 1, 2), fieldNumbers);
        assertEquals(TarantoolNullField.INSTANCE, operations.asList().get(2).getValue());
    }

    @Test
    public void test_tupleOperations_shouldSerializeCorrectly_ifFieldIsDefinedByName() {
        final String FIELD_DATA = "data";
        List<String> object = Arrays.asList("test1", "test2");

        final String FIELD_TS = "ts";
        long epochSecond = 12345L;

        final String FIELD_SPLICE = "test";

        TupleOperations tupleOperations = TupleOperations
            .set(FIELD_DATA, object)
            .andSet(FIELD_TS, epochSecond)
            .andSplice(FIELD_SPLICE, 1, 2, "rep");

        Value value = mapperFactory.defaultComplexTypesMapper().toValue(tupleOperations.asProxyOperationList());

        Value cond1 = ValueFactory.newArray(
            ValueFactory.newString("="),
            ValueFactory.newString(FIELD_DATA),
            ValueFactory.newArray(ValueFactory.newString(object.get(0)), ValueFactory.newString(object.get(1))));

        Value cond2 = ValueFactory.newArray(
            ValueFactory.newString("="),
            ValueFactory.newString(FIELD_TS),
            ValueFactory.newInteger(epochSecond));

        Value cond3 = ValueFactory.newArray(
            ValueFactory.newString(":"),
            ValueFactory.newString(FIELD_SPLICE),
            ValueFactory.newInteger(1),
            ValueFactory.newInteger(2),
            ValueFactory.newString("rep"));

        Value expected = ValueFactory.newArray(cond1, cond2, cond3);

        assertEquals(expected, value);
    }

    @Test
    public void test_cloneWithIndex_shouldCloneCorrectly() {
        TupleOperations operations = TupleOperations
            .add("field_1", 1)
            .andBitwiseAnd("field_2", 2)
            .andBitwiseOr("field_3", 3)
            .andBitwiseXor("field_4", 4)
            .andDelete("field5", 5)
            .andInsert("field_6", 6)
            .andSet("field_7", 7)
            .andSubtract("field_8", 8)
            .andSplice("field_9", 9, 9, "Hello");
        operations.asList().forEach(tupleOperation -> {
            TupleOperation clonedTupleOperation = tupleOperation.cloneWithIndex(1);
            assertNotEquals(tupleOperation, clonedTupleOperation);
            assertNull(tupleOperation.getFieldIndex());
            assertEquals(1, clonedTupleOperation.getFieldIndex());
        });
    }
}
