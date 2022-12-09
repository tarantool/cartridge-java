package io.tarantool.driver.core.metadata;

import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.util.Iterator;

/**
 * Populates space metadata. Input is expected to have the _vspace format.
 * See
 * <a href="https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/_vspace/">
 * https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/_vspace/
 * </a>
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class VSpaceToTarantoolSpaceMetadataConverter
    implements ValueConverter<ArrayValue, TarantoolSpaceMetadata> {

    private static final long serialVersionUID = 3251153816139308575L;

    private static final VSpaceToTarantoolSpaceMetadataConverter instance =
        new VSpaceToTarantoolSpaceMetadataConverter();

    private static final MessagePackMapper mapper =
        DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();

    private static final ArrayValueToSpaceFormatConverter arrayValueToSpaceFormatConverter
        = ArrayValueToSpaceFormatConverter.getInstance();

    private VSpaceToTarantoolSpaceMetadataConverter() {
    }

    public TarantoolSpaceMetadata fromValue(ArrayValue space) {
        Iterator<Value> it = space.iterator();

        TarantoolSpaceMetadataImpl spaceMetadata = new TarantoolSpaceMetadataImpl();

        spaceMetadata.setSpaceId(mapper.fromValue(it.next().asIntegerValue()));
        spaceMetadata.setOwnerId(mapper.fromValue(it.next().asIntegerValue()));
        spaceMetadata.setSpaceName(mapper.fromValue(it.next().asStringValue()));

        Value spaceMetadataValue = it.next();
        while (!spaceMetadataValue.isArrayValue()) {
            spaceMetadataValue = it.next();
        }

        spaceMetadata.setSpaceFormatMetadata(
            arrayValueToSpaceFormatConverter.fromValue(
                spaceMetadataValue.asArrayValue()
            )
        );

        return spaceMetadata;
    }

    public static VSpaceToTarantoolSpaceMetadataConverter getInstance() {
        return instance;
    }
}
