package io.tarantool.driver.api.space.options.crud.enums;

/**
 * Enum represents the CRUD predefined mode option values.
 *
 * @author Belonogov Nikolay.
 * @see <a href="https://github.com/tarantool/crud">tarantool/crud</a>.
 */
public enum Mode {
    WRITE("write"),
    READ("read");

    private final String value;

    Mode(String value) {
        this.value = value;
    }

    public String value() {
        return this.value;
    }

    @Override
    public String toString() {
        return this.value;
    }
}
