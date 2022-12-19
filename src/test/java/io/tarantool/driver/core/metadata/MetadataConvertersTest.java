package io.tarantool.driver.core.metadata;

import io.tarantool.driver.api.metadata.TarantoolFieldMetadata;
import io.tarantool.driver.api.metadata.TarantoolMetadataContainer;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import org.junit.jupiter.api.Test;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.ValueFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Artyom Dubinin
 */
public class MetadataConvertersTest {

    static int UNKNOWN_ID = -1;
    static String customSpaceName = "custom_space_name";
    static String position = "position";
    static String name = "name";
    static String is_nullable = "is_nullable";
    static String type = "type";
    static List<Map<String, Object>> fields;
    static ArrayValue format;

    static {
        Map<String, Object> id = new HashMap<String, Object>() {{
            put(position, 0);
            put(name, "id");
            put(is_nullable, false);
            put(type, "unsigned");
        }};
        MapValue mpId = ValueFactory.newMap(
            ValueFactory.newString(name), ValueFactory.newString((String) id.get(name)),
            ValueFactory.newString(is_nullable), ValueFactory.newBoolean((boolean) id.get(is_nullable)),
            ValueFactory.newString(type), ValueFactory.newString((String) id.get(type))
        );

        Map<String, Object> key = new HashMap<String, Object>() {{
            put(position, 1);
            put(name, "key");
            put(is_nullable, true);
            put(type, "string");
        }};
        MapValue mpKey = ValueFactory.newMap(
            ValueFactory.newString(name), ValueFactory.newString((String) key.get(name)),
            ValueFactory.newString(is_nullable), ValueFactory.newBoolean((Boolean) key.get(is_nullable)),
            ValueFactory.newString(type), ValueFactory.newString((String) key.get(type))
        );
        format = ValueFactory.newArray(mpId, mpKey);
        fields = new ArrayList<>(Arrays.asList(
            key, id
        ));
    }

    /*
    format = {
        spaces = {
            [space_name] = {
                engine = 'vinyl' | 'memtx',
                is_local = true | false,
                temporary = true | false,
                format = {
                    {
                        name = '...',
                        is_nullable = true | false,
                        type = 'unsigned' | 'string' | 'varbinary' |
                            'integer' | 'number' | 'boolean' |
                            'array' | 'scalar' | 'any' | 'map' |
                            'decimal' | 'double' | 'uuid' | 'datetime'
                    },
                    ...
                }
            }
        }
    }
    */
    @Test
    public void testDDLConverter() {
        MapValue space = ValueFactory.newMap(
            ValueFactory.newString("engine"), ValueFactory.newString("memtx"),
            ValueFactory.newString("is_local"), ValueFactory.newBoolean(false),
            ValueFactory.newString("temporary"), ValueFactory.newBoolean(false),
            ValueFactory.newString("format"), format
        );

        MapValue ddlFormat = ValueFactory.newMap(
            ValueFactory.newString("spaces"), ValueFactory.newMap(
                ValueFactory.newString(customSpaceName), space
            )
        );

        TarantoolMetadataContainer metadataContainer = DDLTarantoolSpaceMetadataConverter.getInstance()
            .fromValue(ddlFormat);
        TarantoolSpaceMetadata spaceMetadata = metadataContainer.getSpaceMetadataByName().get(customSpaceName);

        assertEquals(0, spaceMetadata.getSpaceId());
        assertEquals(UNKNOWN_ID, spaceMetadata.getOwnerId());
        assertEquals(customSpaceName, spaceMetadata.getSpaceName());
        Map<String, TarantoolFieldMetadata> spaceFormat = spaceMetadata.getSpaceFormatMetadata();

        for (Map<String, Object> field :
            fields) {
            Object fieldName = field.get(name);
            assertTrue(spaceFormat.containsKey(fieldName));
            TarantoolFieldMetadata fieldMetadata = spaceFormat.get(fieldName);

            assertEquals(field.get(position), fieldMetadata.getFieldPosition());
            assertEquals(field.get(name), fieldMetadata.getFieldName());
            assertEquals(field.get(type), fieldMetadata.getFieldType());
            assertEquals(field.get(is_nullable), fieldMetadata.getIsNullable());
        }
    }

    /*
    - [272, 1, '_schema', 'memtx', 0, {}, [{'type': 'string', 'name': 'key'}, {'type': 'any',
        'name': 'value', 'is_nullable': true}]]
    */
    @Test
    public void testVSpaceConverter() {
        String engine = "memtx";
        int spaceId = 272;
        int ownerId = 1;
        int fieldCount = 0;
        Map flags = new HashMap<>();
        ArrayValue space = ValueFactory.newArray(
            ValueFactory.newInteger(spaceId),
            ValueFactory.newInteger(ownerId),
            ValueFactory.newString(customSpaceName),
            ValueFactory.newString(engine),
            ValueFactory.newInteger(fieldCount),
            ValueFactory.newMap(flags),
            format
        );

        TarantoolSpaceMetadata metadata = VSpaceToTarantoolSpaceMetadataConverter.getInstance().fromValue(space);

        assertEquals(spaceId, metadata.getSpaceId());
        assertEquals(ownerId, metadata.getOwnerId());
        assertEquals(customSpaceName, metadata.getSpaceName());
        Map<String, TarantoolFieldMetadata> format = metadata.getSpaceFormatMetadata();

        for (Map<String, Object> field :
            fields) {
            Object fieldName = field.get(name);
            assertTrue(format.containsKey(fieldName));
            TarantoolFieldMetadata fieldMetadata = format.get(fieldName);

            assertEquals(field.get(position), fieldMetadata.getFieldPosition());
            assertEquals(field.get(name), fieldMetadata.getFieldName());
            assertEquals(field.get(type), fieldMetadata.getFieldType());
            assertEquals(field.get(is_nullable), fieldMetadata.getIsNullable());
        }
    }

    /*
    - metadata:
      - {'name': 'id', 'type': 'unsigned'}
      - {'name': 'bucket_id', 'type': 'unsigned'}
      - {'name': 'name', 'type': 'string'}
      - {'name': 'age', 'type': 'number'}
    */
    @Test
    public void testCRUDResponseConverter() {
        TarantoolSpaceMetadata metadata = CRUDResponseToTarantoolSpaceMetadataConverter
            .getInstance().fromValue(format);

        assertEquals(0, metadata.getSpaceId());
        assertEquals(0, metadata.getOwnerId());
        assertNull(metadata.getSpaceName());
        Map<String, TarantoolFieldMetadata> format = metadata.getSpaceFormatMetadata();

        for (Map<String, Object> field :
            fields) {
            Object fieldName = field.get(name);
            assertTrue(format.containsKey(fieldName));
            TarantoolFieldMetadata fieldMetadata = format.get(fieldName);

            assertEquals(field.get(position), fieldMetadata.getFieldPosition());
            assertEquals(field.get(name), fieldMetadata.getFieldName());
            assertEquals(field.get(type), fieldMetadata.getFieldType());
            assertEquals(field.get(is_nullable), fieldMetadata.getIsNullable());
        }
    }
}
