package io.tarantool.driver.protocol;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the Tarantool packet frame header.
 * See <a href="https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#box-protocol-header">
 *     https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#box-protocol-header</a>
 *
 * @author Alexey Kuzin
 */
public final class TarantoolHeader implements Packable {

    private static final int IPROTO_REQUEST_TYPE = 0x00;
    private static final int IPROTO_SYNC = 0x01;
    private static final int IPROTO_REPLICA_ID = 0x02;
    private static final int IPROTO_LSN = 0x03;
    private static final int IPROTO_TIMESTAMP = 0x04;
    private static final int IPROTO_SCHEMA_VERSION = 0x05;

    private Long sync;
    private Long code;
    private Long schemaVersion;
    private Long replicaId;
    private Long lsn;
    private Double timestamp;

    private TarantoolHeader() {
    }

    TarantoolHeader(Long sync, Long code) {
        this.sync = sync;
        this.code = code;
    }

    TarantoolHeader(Long sync, Long code, Long schemaVersion) {
        this.sync = sync;
        this.code = code;
        this.schemaVersion = schemaVersion;
    }

    public void setSync(Long sync) {
        this.sync = sync;
    }

    public Long getSync() {
        return sync;
    }

    public void setCode(Long code) {
        this.code = code;
    }

    public Long getCode() {
        return code;
    }

    public void setSchemaVersion(Long schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public Long getSchemaVersion() {
        return schemaVersion;
    }

    public void setReplicaId(Long replicaId) {
        this.replicaId = replicaId;
    }

    public Long getReplicaId() {
        return replicaId;
    }

    public Long getLsn() {
        return lsn;
    }

    public void setLsn(Long lsn) {
        this.lsn = lsn;
    }

    public Double getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Double timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Converts the current header contents into a MessagePack {@link Value}
     * @return MessagePack representation of the header
     */
    public Value toMessagePackValue(MessagePackObjectMapper mapper) {
        Map<IntegerValue, IntegerValue> values = new HashMap<>();
        values.put(ValueFactory.newInteger(IPROTO_REQUEST_TYPE), ValueFactory.newInteger(code));
        values.put(ValueFactory.newInteger(IPROTO_SYNC), ValueFactory.newInteger(sync));
        if (schemaVersion != null) {
            values.put(ValueFactory.newInteger(IPROTO_SCHEMA_VERSION), ValueFactory.newInteger(schemaVersion));
        }
        return ValueFactory.newMap(values);
    }

    /**
     * Creates an instance of {@link TarantoolHeader} from MessagePack {@link Value}
     * @param value must be an instance of {@link MapValue}
     * @return a {@link TarantoolHeader} instance
     * @throws TarantoolProtocolException if the passed value is not a {@link MapValue}, mandatory fields are absent
     * or have wrong type
     */
    public static TarantoolHeader fromMessagePackValue(Value value) throws TarantoolProtocolException {
        if (!value.isMapValue()) {
            throw new TarantoolProtocolException("TarantoolHeader can be unpacked only from MP_MAP, received "
                    + value);
        }
        Map<Value, Value> values = value.asMapValue().map();
        TarantoolHeader header = new TarantoolHeader();
        for (Map.Entry<Value, Value> entry : values.entrySet()) {
            Value key = entry.getKey();
            if (!key.isIntegerValue()) {
                throw new TarantoolProtocolException("TarantoolHeader keys must be of MP_INT type");
            }
            Value field = entry.getValue();
            if (!field.isNumberValue()) {
                throw new TarantoolProtocolException("TarantoolHeader values must be of MP_INT type");
            }
            int keyInt = key.asIntegerValue().asInt();
            switch (keyInt) {
                case IPROTO_REQUEST_TYPE:
                    header.setCode(field.asIntegerValue().asLong());
                    break;
                case IPROTO_SYNC:
                    header.setSync(field.asIntegerValue().asLong());
                    break;
                case IPROTO_SCHEMA_VERSION:
                    header.setSchemaVersion(field.asIntegerValue().asLong());
                    break;
                case IPROTO_LSN:
                    header.setLsn(field.asIntegerValue().asLong());
                    break;
                case IPROTO_REPLICA_ID:
                    header.setReplicaId(field.asIntegerValue().asLong());
                    break;
                case IPROTO_TIMESTAMP:
                    header.setTimestamp(field.asFloatValue().toDouble());
                    break;
                default:
                    throw new IllegalStateException("unexpected IPROTO type key " + keyInt);
            }
        }
        if (header.getCode() == null) {
            throw new TarantoolProtocolException("No request or response code found");
        }
        if (header.getSync() == null) {
            throw new TarantoolProtocolException("No sync ID found");
        }
        return header;
    }
}
