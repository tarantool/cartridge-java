package org.testcontainers.containers;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * Sets up a Tarantool instance and provides API for configuring it
 *
 * @author Alexey Kuzin
 */
public class TarantoolContainer extends GenericContainer<TarantoolContainer> {
    public static final String TARANTOOL_IMAGE = "tarantool/tarantool";
    public static final String DEFAULT_IMAGE_VERSION = "2.x-centos7";
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 3301;
    private static final String SCRIPT_RESOURCE_PATH = "org/testcontainers/containers/server.lua";
    private static final String INSTANCE_DIR = "/app";
    private static final String INSTANCE_PATH = INSTANCE_DIR + "/server.lua";

    private static final Logger log = LoggerFactory.getLogger(TarantoolContainer.class);
    private static final String API_USER = "api_user";
    private static final String API_PASSWORD = "secret";
    private Integer port;

    public TarantoolContainer() {
        this(String.format("%s:%s", TARANTOOL_IMAGE, DEFAULT_IMAGE_VERSION));
    }

    public TarantoolContainer(String dockerImageName) {
        super(new ImageFromDockerfile()
                .withDockerfileFromBuilder(builder ->
                    builder
                        .from(dockerImageName)
                        .volume(INSTANCE_DIR)
                        .cmd("tarantool", INSTANCE_PATH))
        );
        withClasspathResourceMapping(SCRIPT_RESOURCE_PATH, INSTANCE_PATH, BindMode.READ_ONLY);
        withExposedPorts(DEFAULT_PORT);
        withLogConsumer(new Slf4jLogConsumer(log));
        waitingFor(Wait.forLogMessage(".*entering the event loop.*", 1));
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo, boolean reused) {
        super.containerIsStarted(containerInfo, reused);
        port = getMappedPort(DEFAULT_PORT);
    }

    @Override
    public String getHost() {
        return DEFAULT_HOST;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return API_USER;
    }

    public String getPassword() {
        return API_PASSWORD;
    }
}
