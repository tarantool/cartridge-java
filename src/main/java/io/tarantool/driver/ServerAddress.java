package io.tarantool.driver;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * Represents the location of a Tarantool server - server name and port number
 *
 * @author Sergey Volgin
 */
public class ServerAddress {

    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_PORT = 3301;

    private final String host;
    private final int port;

    /**
     * Create a ServerAddress with default host and port
     */
    public ServerAddress() {
        this(DEFAULT_HOST, DEFAULT_PORT);
    }

    /**
     * Create a ServerAddress with default port
     *
     * @param host hostname
     */
    public ServerAddress(final String host) {
        this(host, DEFAULT_PORT);
    }

    /**
     * Create a {@link ServerAddress} instance
     *
     * @param host hostname
     * @param port tarantool port
     */
    public ServerAddress(final String host, final int port) {
        int portNumber = port;
        String hostName = host;

        int firstIndex = host.indexOf(":");
        int lastIndex = host.lastIndexOf(":");
        if (firstIndex > 0 && firstIndex == lastIndex) {
            if (port != DEFAULT_PORT) {
                throw new IllegalArgumentException("can't specify port in construct and in host name");
            }
            try {
                portNumber = Integer.parseInt(hostName.substring(firstIndex + 1));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid port number: host and port should be specified in host:port format");
            }
            if (portNumber <= 0 || portNumber > 65535) {
                throw new IllegalArgumentException(String.format("Invalid port number : %s", portNumber));
            }
            hostName = host.substring(0, firstIndex).trim();
        }

        this.port = portNumber;
        this.host = hostName;
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

        ServerAddress that = (ServerAddress) o;
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
