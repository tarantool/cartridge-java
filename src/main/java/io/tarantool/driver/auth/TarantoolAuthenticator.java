package io.tarantool.driver.auth;

/**
 * Prepares authentication data for passing to Tarantool
 *
 * @author Alexey Kuzin
 */
public interface TarantoolAuthenticator<T extends TarantoolCredentials> {

    /**
     * Return the authentication mechanism signature
     * @return
     * @see TarantoolAuthMechanism
     */
    TarantoolAuthMechanism getMechanism();

    /**
     * Check if the passed instance of {@link TarantoolCredentials} can be used for authentication
     * @return {@code true} if the credentials data are sufficient for performing authentication
     */
    boolean canAuthenticateWith(T credentials);

    /**
     * Takes the server auth data returned in response for the connect request and user auth data, performs
     * the necessary transformations and writes the serialized authentication data to a byte array
     *
     * @param serverAuthData bytes with auth data from the Tarantool server greeting
     * @param credentials Tarantool user credentials
     */
    byte[] prepareUserAuthData(byte[] serverAuthData, T credentials) throws TarantoolAuthenticationException;
}
