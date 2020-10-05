package io.tarantool.driver;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents the Tarantool server version
 *
 * @author Alexey Kuzin
 */
public class TarantoolVersion implements Serializable {
    private static final long serialVersionUID = 87703595811996764L;
    private final String fullVersion;

    /**
     * Constructor
     * @param versionString a string containing Tarantool server version
     */
    TarantoolVersion(String versionString) {
        fullVersion = versionString;
    }

    /**
     * Constructs version from a string
     * @param versionString a string containing Tarantool server version
     * @return new {@link TarantoolVersion} instance incapsulating the specified version
     * @throws InvalidVersionException if the passed version string is invalid
     */
    public static TarantoolVersion fromString(String versionString) throws InvalidVersionException {
        if (versionString == null || !versionString.startsWith("Tarantool ")) {
            throw new InvalidVersionException(versionString);
        }
        return new TarantoolVersion(versionString.trim());
    }

    @Override
    public String toString() {
        return fullVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TarantoolVersion that = (TarantoolVersion) o;
        return Objects.equals(fullVersion, that.fullVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fullVersion);
    }
}
