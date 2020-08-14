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

    TarantoolOperationType getOperationType();

    Integer getFieldNumber();

    String getFieldName();

    void setFieldNumber(Integer fieldNumber);
}
