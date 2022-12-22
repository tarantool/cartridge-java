package io.tarantool.driver.api.space.options;

import java.util.List;
import java.util.Optional;

/**
 * Base interface for all operation options that may have a configurable return.
 *
 * @author Artyom Dubinin
 */
public interface OperationWithFieldsOptions<T extends OperationWithFieldsOptions<T>>
    extends Options, Self<T> {

    String FIELDS = "fields";

    /**
     * Specifies list of fields names for getting only a subset of fields.
     * By default, all fields are returned.
     *
     * @param fields list of string field names
     * @return this options instance
     */
    default T withFields(List<String> fields) {
        addOption(FIELDS, fields);
        return self();
    }

    /**
     * Return list of fields names for getting only a subset of fields.
     *
     * @return list of fields string names
     */
    default Optional<List> getFields() {
        return getOption(FIELDS, List.class);
    }
}
