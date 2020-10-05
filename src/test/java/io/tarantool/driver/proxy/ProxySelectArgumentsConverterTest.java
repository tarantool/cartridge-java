package io.tarantool.driver.proxy;

import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.metadata.TarantoolFieldFormatMetadata;
import io.tarantool.driver.metadata.TarantoolIndexPartMetadata;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.protocol.TarantoolIteratorType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProxySelectArgumentsConverterTest {

    @Test
    public void testEqualQuery() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method setFormatMethod =
                TarantoolSpaceMetadata.class.getDeclaredMethod("setSpaceFormatMetadata", LinkedHashMap.class);
        setFormatMethod.setAccessible(true);

        TarantoolSpaceMetadata spaceMetadata = new TarantoolSpaceMetadata();
        LinkedHashMap<String, TarantoolFieldFormatMetadata> formatMetadata = new LinkedHashMap<>();
        formatMetadata.put("id", new TarantoolFieldFormatMetadata("id", "unsigned", 0));
        formatMetadata.put("book_name",
                new TarantoolFieldFormatMetadata("book_name", "string", 1));
        setFormatMethod.invoke(spaceMetadata, formatMetadata);

        List<TarantoolIndexPartMetadata> indexPartMetadata = new ArrayList<>();
        indexPartMetadata.add(new TarantoolIndexPartMetadata(0, "unsigned"));
        indexPartMetadata.add(new TarantoolIndexPartMetadata(1, "string"));

        TarantoolIndexQuery indexQuery = new TarantoolIndexQuery();
        indexQuery.withKeyValues(Arrays.asList(10, "abc"));

        List<?> selectArguments =
                ProxySelectArgumentsConverter.fromIndexQuery(indexQuery, indexPartMetadata, spaceMetadata);

        assertEquals(selectArguments, Arrays.asList(
                Arrays.asList(ProxySelectArgumentsConverter.EQ, "id", 10),
                Arrays.asList(ProxySelectArgumentsConverter.EQ, "book_name", "abc")
        ));
    }

    @Test
    public void testIteratorsQuery() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method setFormatMethod =
                TarantoolSpaceMetadata.class.getDeclaredMethod("setSpaceFormatMetadata", LinkedHashMap.class);
        setFormatMethod.setAccessible(true);

        TarantoolSpaceMetadata spaceMetadata = new TarantoolSpaceMetadata();
        LinkedHashMap<String, TarantoolFieldFormatMetadata> formatMetadata = new LinkedHashMap<>();
        formatMetadata.put("id", new TarantoolFieldFormatMetadata("id", "unsigned", 0));
        formatMetadata.put("book_name",
                new TarantoolFieldFormatMetadata("book_name", "string", 1));
        setFormatMethod.invoke(spaceMetadata, formatMetadata);

        List<TarantoolIndexPartMetadata> indexPartMetadata = new ArrayList<>();
        indexPartMetadata.add(new TarantoolIndexPartMetadata(0, "unsigned"));

        TarantoolIndexQuery indexQuery = new TarantoolIndexQuery();
        indexQuery.withKeyValues(Collections.singletonList(10));
        indexQuery.withIteratorType(TarantoolIteratorType.ITER_GT);

        List<?> selectArguments =
                ProxySelectArgumentsConverter.fromIndexQuery(indexQuery, indexPartMetadata, spaceMetadata);

        assertEquals(selectArguments, Collections.singletonList(
                Arrays.asList(ProxySelectArgumentsConverter.GT, "id", 10)
        ));

        indexQuery.withIteratorType(TarantoolIteratorType.ITER_GE);
        selectArguments = ProxySelectArgumentsConverter.fromIndexQuery(indexQuery, indexPartMetadata, spaceMetadata);
        assertEquals(selectArguments, Collections.singletonList(
                Arrays.asList(ProxySelectArgumentsConverter.GE, "id", 10)
        ));

        indexQuery.withIteratorType(TarantoolIteratorType.ITER_LE);
        selectArguments = ProxySelectArgumentsConverter.fromIndexQuery(indexQuery, indexPartMetadata, spaceMetadata);
        assertEquals(selectArguments, Collections.singletonList(
                Arrays.asList(ProxySelectArgumentsConverter.LE, "id", 10)
        ));

        indexQuery.withIteratorType(TarantoolIteratorType.ITER_LT);
        selectArguments = ProxySelectArgumentsConverter.fromIndexQuery(indexQuery, indexPartMetadata, spaceMetadata);
        assertEquals(selectArguments, Collections.singletonList(
                Arrays.asList(ProxySelectArgumentsConverter.LT, "id", 10)
        ));
    }
}
