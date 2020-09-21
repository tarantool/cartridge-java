package io.tarantool.driver;

import io.tarantool.driver.exceptions.TarantoolSocketException;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * Represents the location of a Tarantool server - server name and port number
 *
 * @author Sergey Volgin
 */
public class TarantoolServerAddress implements Serializable {

    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 3301;
    private static final long serialVersionUID = 7327851568010264254L;

    private final String host;
    private final int port;

    /**
     * Create a TarantoolServerAddress with default host and port
     */
    public TarantoolServerAddress() {
        this(DEFAULT_HOST, DEFAULT_PORT);
    }

    /**
     * Create a TarantoolServerAddress with default port
     *
     * @param host hostname
     */
    public TarantoolServerAddress(final String host) {
        this(host, DEFAULT_PORT);
    }

    /**
     * Create a {@link TarantoolServerAddress} instance
     *
     * @param host hostname
     * @param port tarantool port
     */
    public TarantoolServerAddress(final String host, final int port) {
        //discard username if specified
        String[] parts = host.split("@");
        String[] addressParts = parts[parts.length - 1].split(":");

        if (addressParts.length > 2) {
            throw new IllegalArgumentException(String.format("Invalid host name: %s", host));
        }

        int portNumber = port;
        if (addressParts.length == 2) {
            try {
                portNumber = Integer.parseInt(addressParts[1]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(String.format("Invalid host name: %s", host));
            }
        }

        if (portNumber <= 0 || portNumber > 65535) {
            throw new IllegalArgumentException(String.format("Invalid port number : %s", portNumber));
        }

        this.host = addressParts[0];
        this.port = portNumber;
    }

    /**
     * Auxiliary constructor for conversion between {@link InetSocketAddress} and {@link TarantoolServerAddress}
     * @param socketAddress remote server address
     */
    public TarantoolServerAddress(InetSocketAddress socketAddress) {
        this.host = socketAddress.getHostName();
        this.port = socketAddress.getPort();
    }

    /**
     * Get the hostname
     *
     * @return hostname
     */
    public String getHost() {
        return host;
    }

    /**
     * Get the port number
     *
     * @return port
     */
    public int getPort() {
        return port;
    }

    /**
     * Get the socket address
     *
     * @return socket address
     */
    public InetSocketAddress getSocketAddress() throws TarantoolSocketException {
        try {
            return new InetSocketAddress(InetAddress.getByName(host), port);
        } catch (UnknownHostException e) {
            throw new TarantoolSocketException(e.getMessage(), this, e);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TarantoolServerAddress that = (TarantoolServerAddress) o;
        if (port != that.port) {
            return false;
        }

        return host.equals(that.host);
    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + port;
        return result;
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
