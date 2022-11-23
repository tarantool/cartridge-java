package io.tarantool.driver.cluster;

import java.util.Objects;

/**
 * <p>This class is not part of the public API.</p>
 *
 * @author Sergey Volgin
 */
final class ServerNodeInfo {

    private static final String STATUS_AVAILABLE = "available";
    private static final String STATUS_HEALTHY = "healthy";

    private String uuid;
    private String uri;
    private String status;
    private Integer priority;

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

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public boolean isAvailable() {
        return this.status.equals(STATUS_AVAILABLE) || this.status.equals(STATUS_HEALTHY);
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
            Objects.equals(priority, that.priority);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid, uri, status, priority);
    }
}
