package io.tarantool.driver.protocol.operations;

import io.tarantool.driver.protocol.Packable;

/**
 * Base interface of tuple operation for
 * {@link io.tarantool.driver.protocol.requests.TarantoolUpdateRequest} and
 * {@link io.tarantool.driver.protocol.requests.TarantoolUpsertRequest}
 *
 * @author Sergey Volgin
 */
public interface TupleOperation extends Packable {

    TarantoolUpdateOperationType getOperationType();

    Integer getFieldIndex();

    String getFieldName();

    void setFieldIndex(Integer fieldIndex);
}
