package io.tarantool.driver;

import io.tarantool.driver.utils.Assert;
import org.testcontainers.shaded.org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public final class TarantoolUtils {

    private static final String TARANTOOL_VERSION = "TARANTOOL_VERSION";

    private TarantoolUtils() {
    }

    public static boolean versionGreaterThen(String tarantoolVersion) {
        Assert.notNull(tarantoolVersion, "tarantoolVersion must not be null");
        String tarantoolCiVersion = java.lang.System.getenv(TARANTOOL_VERSION);
        if (StringUtils.isEmpty(tarantoolCiVersion)) {
            return true;
        }
        TarantoolVersion ciVersion = new TarantoolVersion(tarantoolCiVersion);
        TarantoolVersion version = new TarantoolVersion(tarantoolVersion);
        return ciVersion.getMajor() > version.getMajor() &&
            ciVersion.getMinor() > version.getMinor();
    }

    public static boolean versionWithUUID() {
        return versionGreaterThen("2.4");
    }

    public static boolean versionWithVarbinary() {
        return versionGreaterThen("2.2.1");
    }

    public static class TarantoolVersion {
        private final Integer major;
        private final Integer minor;

        public TarantoolVersion(String stringVersion) {
            List<Integer> majorMinor = StringUtils.isEmpty(stringVersion) ? Collections.emptyList() :
                Arrays.stream(stringVersion.split("\\."))
                    .map(Integer::parseInt)
                    .collect(Collectors.toList());
            major = majorMinor.size() >= 1 ? majorMinor.get(0) : 0;
            minor = majorMinor.size() >= 2 ? majorMinor.get(1) : 0;
        }

        public Integer getMajor() {
            return major;
        }

        public Integer getMinor() {
            return minor;
        }
    }

    public static Integer DEFAULT_RETRYING_ATTEMPTS = 5;
    public static Integer DEFAULT_RETRYING_DELAY = 100;

    public static void retry(Runnable fn) throws InterruptedException {
        retry(DEFAULT_RETRYING_ATTEMPTS, DEFAULT_RETRYING_DELAY, fn);
    }

    public static void retry(Integer attempts, Integer delay, Runnable fn) throws InterruptedException {
        while (attempts > 0) {
            try {
                fn.run();
                return;
            } catch (AssertionError ignored) {
            }

            --attempts;
            Thread.sleep(delay);
        }
        fn.run();
    }
}
