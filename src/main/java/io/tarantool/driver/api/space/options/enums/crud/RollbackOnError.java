package io.tarantool.driver.api.space.options.enums.crud;

/**
 * Enum represents the CRUD predefined rollback_on_error option values.
 *
 * @author Belonogov Nikolay.
 * @see <a href="https://github.com/tarantool/crud">tarantool/crud</a>.
 */
public enum RollbackOnError {

    TRUE(true),

    FALSE(false);

    public static final String NAME = "rollback_on_error";
    private final boolean value;

    RollbackOnError(boolean value) {
        this.value = value;
    }

    public boolean value() {
        return this.value;
    }

    @Override
    public String toString() {
        return Boolean.toString(this.value);
    }
}
