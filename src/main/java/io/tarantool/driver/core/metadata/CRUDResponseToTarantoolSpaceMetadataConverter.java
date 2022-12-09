package io.tarantool.driver.core.metadata;

import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;

/**
 * Populates crud response metadata format to {@link TarantoolSpaceMetadata}.
 * The input data must be consistent with the metadata format from tarantool/crud.
 * See
 * <a href="https://github.com/tarantool/crud#api">
 * https://github.com/tarantool/crud#api
 * </a>
 *
 * @author Artyom Dubinin
 */
public final class CRUDResponseToTarantoolSpaceMetadataConverter
    implements ValueConverter<ArrayValue, TarantoolSpaceMetadata> {

    private static final CRUDResponseToTarantoolSpaceMetadataConverter instance =
        new CRUDResponseToTarantoolSpaceMetadataConverter();

    private static final ArrayValueToSpaceFormatConverter arrayValueToSpaceFormatConverter
        = ArrayValueToSpaceFormatConverter.getInstance();

    private CRUDResponseToTarantoolSpaceMetadataConverter() {
    }

    public TarantoolSpaceMetadata fromValue(ArrayValue metadata) {
        TarantoolSpaceMetadataImpl spaceMetadata = new TarantoolSpaceMetadataImpl();
        spaceMetadata.setSpaceFormatMetadata(arrayValueToSpaceFormatConverter.fromValue(metadata));
        return spaceMetadata;
    }

    public static CRUDResponseToTarantoolSpaceMetadataConverter getInstance() {
        return instance;
    }
}
