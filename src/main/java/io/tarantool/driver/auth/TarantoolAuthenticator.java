package io.tarantool.driver.auth;

/**
 * Prepares authentication data for passing to Tarantool
 *
 * @param <T> user credentials type
 * @author Alexey Kuzin
 */
public interface TarantoolAuthenticator<T extends TarantoolCredentials> {

    /**
     * Return the authentication mechanism signature
     * @return authentication mechanism instance
     * @see TarantoolAuthMechanism
     */
    TarantoolAuthMechanism getMechanism();

    /**
     * Check if the passed instance of {@link TarantoolCredentials} can be used for authentication
     * @param credentials Tarantool user credentials
     * @return {@code true} if the credentials data are sufficient for performing authentication
     */
    boolean canAuthenticateWith(T credentials);

    /**
     * Takes the server auth data returned in response for the connect request and user auth data, performs
     * the necessary transformations and writes the serialized authentication data to a byte array
     * @param serverAuthData bytes with auth data from the Tarantool server greeting
     * @param credentials Tarantool user credentials
     * @return the auth data in the form of byte array, ready to be transferred in an authentication request to
     * Tarantool server
     * @throws TarantoolAuthenticationException id authentication failed
     * @see <a
     * href="https://www.tarantool.io/en/doc/latest/dev_guide/internals/box_protocol/#binary-protocol-authentication">
     *     https://www.tarantool.io/en/doc/latest/dev_guide/internals/box_protocol/#binary-protocol-authentication</a>
     */
    byte[] prepareUserAuthData(byte[] serverAuthData, T credentials) throws TarantoolAuthenticationException;
}
