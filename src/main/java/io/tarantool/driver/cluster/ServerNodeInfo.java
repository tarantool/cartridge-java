package io.tarantool.driver.cluster;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * <p>This class is not part of the public API.</p>
 *
 * @author Sergey Volgin
 */
final class ServerNodeInfo {
    private String uuid;
    private String uri;
    private String status;
    @JsonProperty("network_timeout")
    private Double networkTimeout;

    ServerNodeInfo() {
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

    public Double getNetworkTimeout() {
        return networkTimeout;
    }

    public void setNetworkTimeout(Double networkTimeout) {
        this.networkTimeout = networkTimeout;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ServerNodeInfo that = (ServerNodeInfo) o;
        return Objects.equals(uuid, that.uuid) &&
                Objects.equals(uri, that.uri) &&
                Objects.equals(status, that.status) &&
                Objects.equals(networkTimeout, that.networkTimeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid, uri, status, networkTimeout);
    }
}
