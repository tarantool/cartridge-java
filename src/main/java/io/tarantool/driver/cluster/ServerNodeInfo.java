package io.tarantool.driver.cluster;

import java.util.Objects;

/**
 * <p>This class is not part of the public API.</p>
 */
final class ServerNodeInfo {
    private String uuid;
    private String uri;
    private String status;
    private Double network_timeout;

    public ServerNodeInfo() {
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Double getNetwork_timeout() {
        return network_timeout;
    }

    public void setNetwork_timeout(Double network_timeout) {
        this.network_timeout = network_timeout;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServerNodeInfo that = (ServerNodeInfo) o;
        return Objects.equals(uuid, that.uuid) &&
                Objects.equals(uri, that.uri) &&
                Objects.equals(status, that.status) &&
                Objects.equals(network_timeout, that.network_timeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid, uri, status, network_timeout);
    }
}
