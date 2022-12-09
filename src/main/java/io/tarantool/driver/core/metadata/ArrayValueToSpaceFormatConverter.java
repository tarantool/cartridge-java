package io.tarantool.driver.core.metadata;

import io.tarantool.driver.api.metadata.TarantoolFieldMetadata;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.impl.ImmutableStringValueImpl;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Populates space format metadata. The input may be a tuple from the internal schema space or from a function response.
 * See
 * <a href="https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/format/">
 * https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/format/
 * </a>
 *
 * @author Artyom Dubinin
 */
public final class ArrayValueToSpaceFormatConverter
    implements ValueConverter<ArrayValue, Map<String, TarantoolFieldMetadata>> {

    private static final StringValue FORMAT_FIELD_NAME = new ImmutableStringValueImpl("name");
    private static final StringValue FORMAT_FIELD_TYPE = new ImmutableStringValueImpl("type");
    private static final StringValue FORMAT_FIELD_IS_NULLABLE = new ImmutableStringValueImpl("is_nullable");

    private static final ArrayValueToSpaceFormatConverter instance = new ArrayValueToSpaceFormatConverter();

    private ArrayValueToSpaceFormatConverter() {
    }

    @Override
    public Map<String, TarantoolFieldMetadata> fromValue(ArrayValue format) {
        Map<String, TarantoolFieldMetadata> spaceFormatMetadata = new LinkedHashMap<>();

        int fieldPosition = 0;
        for (Value fieldValueMetadata : format) {
            Map<Value, Value> fieldMap = fieldValueMetadata.asMapValue().map();
            Optional<Value> isNullable = Optional.ofNullable(fieldMap.get(FORMAT_FIELD_IS_NULLABLE));
            spaceFormatMetadata.put(
                fieldMap.get(FORMAT_FIELD_NAME).toString(),
                new TarantoolFieldMetadataImpl(
                    fieldMap.get(FORMAT_FIELD_NAME).asStringValue().asString(),
                    fieldMap.get(FORMAT_FIELD_TYPE).asStringValue().asString(),
                    fieldPosition,
                    isNullable.isPresent() && isNullable.get().asBooleanValue().getBoolean()
                )
            );
            fieldPosition++;
        }

        return spaceFormatMetadata;
    }

    public static ArrayValueToSpaceFormatConverter getInstance() {
        return instance;
    }
}
