package io.tarantool.driver;

/**
 * Aware of detecting Tarantool versions in the passed source. Stores the version for further use.
 *
 * @author Alexey Kuzin
 */
public class TarantoolVersionHolder {

    private TarantoolVersion tarantoolVersion;

    /**
     * Reads Tarantool version from a {@code String}. The version may be later retrieved using {@link #getVersion()}
     * @param versionString string containing the Tarantool version
     * @throws InvalidVersionException if the version is invalid or unsupported
     */
    public void readVersion(String versionString) throws InvalidVersionException {
        tarantoolVersion = TarantoolVersion.fromString(versionString);
    }

    /**
     * Get Tarantool server version
     * @return an instance of {@link TarantoolVersion}
     */
    public TarantoolVersion getVersion() {
        return tarantoolVersion;
    }
}
