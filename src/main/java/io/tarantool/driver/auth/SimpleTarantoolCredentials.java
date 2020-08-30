package io.tarantool.driver.auth;

import org.springframework.util.Assert;

/**
 * Container for plain user and password data for authentication
 */
public class SimpleTarantoolCredentials implements TarantoolCredentials {

    private static final String DEFAULT_USER = "guest";
    private static final String DEFAULT_PASSWORD = "";

    private final String user;
    private final String password;

    /**
     * Basic constructor.
     * @param user non-empty username
     * @param password non-null password
     */
    public SimpleTarantoolCredentials(String user, String password) {
        Assert.hasText(user, "User must not be empty");
        Assert.notNull(password, "Password must not be null");

        this.user = user;
        this.password = password;
    }

    /**
     * Simple constructor which uses the default guest credentials
     */
    public SimpleTarantoolCredentials() {
        this.user = DEFAULT_USER;
        this.password = DEFAULT_PASSWORD;
    }

    @Override
    public String getUsername() {
        return user;
    }

    public String getPassword() {
        return password;
    }
}
