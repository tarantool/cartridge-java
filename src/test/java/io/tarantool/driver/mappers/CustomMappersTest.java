package io.tarantool.driver.mappers;

import org.junit.jupiter.api.Test;

import org.msgpack.value.ArrayValue;
import org.msgpack.value.StringValue;
import org.msgpack.value.ValueFactory;


import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
public class CustomMappersTest {
    @Test
    void testCustomTimestampMapper() {
        MessagePackMapper defaultMapper =
                DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
        defaultMapper.registerObjectConverter(
                Timestamp.class, StringValue.class, object -> ValueFactory.newString(String.valueOf(object.getTime())));

        Timestamp ts = Timestamp.from(Instant.now());
        List<Object> tuple = Arrays.asList("sbc", 123, Arrays.asList("abc", ts), ts);
        ArrayValue packedTuple = defaultMapper.toValue(tuple);
        assertEquals(tuple.size(), packedTuple.size());
    }
}
