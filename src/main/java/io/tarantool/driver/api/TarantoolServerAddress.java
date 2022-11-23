package io.tarantool.driver.api;

import io.tarantool.driver.exceptions.TarantoolSocketException;

import java.io.Serializable;
import java.net.InetSocketAddress;

/**
 * Represents the location of a Tarantool server - server name and port number
 *
 * @author Sergey Volgin
 * @author Oleg Kuznetsov
 */
public class TarantoolServerAddress implements Serializable {
    private static final long serialVersionUID = 7327851568010264254L;

    private final InetSocketAddress socketAddress;

    /**
     * Creates a TarantoolServerAddress with default host and port
     */
    public TarantoolServerAddress() {
        this("127.0.0.1", 3301);
    }

    /**
     * Creates a {@link TarantoolServerAddress} instance
     *
     * @param host Tarantool server hostname
     * @param port Tarantool server port
     */
    public TarantoolServerAddress(final String host, final int port) {
        this.socketAddress = new InetSocketAddress(host, port);
    }

    /**
     * Create a TarantoolServerAddress from connection string
     * e.g. 127.0.0.1:3301, localhost:3301, [::1]:3301, user:password@localhost:3301, user:password@[::1]:3301
     *
     * @param address address to Tarantool
     */
    public TarantoolServerAddress(final String address) {
        String hostToUse = splitHostByUser(address);
        Integer portToUse = null;
        if (hostToUse.startsWith("[")) {
            int idx = address.indexOf("]");
            if (idx == -1) {
                throw new IllegalArgumentException(
                    "An IPV6 address must be enclosed with '[' and ']' according to RFC 2732.");
            }

            int portIdx = address.indexOf("]:");
            if (portIdx != -1) {
                try {
                    portToUse = Integer.parseInt(address.substring(portIdx + 2));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(String.format("Invalid address: %s", address));
                }
            }
            hostToUse = address.substring(1, idx);
        }

        int idx = hostToUse.indexOf(":");
        int lastIdx = hostToUse.lastIndexOf(":");
        if (idx == lastIdx && idx > 0) {
            try {
                portToUse = Integer.parseInt(hostToUse.substring(idx + 1));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(String.format("Invalid address: %s", address));
            }
            hostToUse = hostToUse.substring(0, idx).trim();
        }

        if (portToUse == null) {
            throw new IllegalArgumentException(String.format("Invalid address: %s", address));
        }

        this.socketAddress = new InetSocketAddress(hostToUse.toLowerCase(), portToUse);
    }

    private String splitHostByUser(String host) {
        String hostToUse = host;
        if (hostToUse == null) {
            throw new IllegalArgumentException("Host is null");
        }

        final String[] split = hostToUse.split("@");

        if (split.length > 2) {
            throw new IllegalArgumentException(
                String.format("Incorrect address for connecting to Tarantool: %s", hostToUse));
        }
        if (split.length == 2) {
            hostToUse = split[1];
        }
        if (split.length == 1) {
            hostToUse = split[0];
        }
        hostToUse = hostToUse.trim();
        if (hostToUse.length() == 0) {
            throw new IllegalArgumentException("Host is empty");
        }

        return hostToUse;
    }

    /**
     * Auxiliary constructor for conversion between {@link InetSocketAddress} and {@link TarantoolServerAddress}
     *
     * @param socketAddress remote server address
     */
    public TarantoolServerAddress(InetSocketAddress socketAddress) {
        this.socketAddress = socketAddress;
    }

    /**
     * Get the hostname
     *
     * @return hostname
     */
    public String getHost() {
        return this.socketAddress.getHostName();
    }

    /**
     * Get the port number
     *
     * @return port
     */
    public int getPort() {
        return this.socketAddress.getPort();
    }

    /**
     * Get the socket address
     *
     * @return socket address
     */
    public InetSocketAddress getSocketAddress() throws TarantoolSocketException {
        return this.socketAddress;
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
        return this.socketAddress.equals(that.socketAddress);
    }

    @Override
    public int hashCode() {
        return this.socketAddress.hashCode();
    }

    @Override
    public String toString() {
        return this.socketAddress.toString();
    }
}
