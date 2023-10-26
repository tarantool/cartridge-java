package io.tarantool.driver;

import io.tarantool.driver.utils.Assert;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public final class TarantoolUtils {

    private static final String TARANTOOL_VERSION = "TARANTOOL_VERSION";

    private TarantoolUtils() {
    }

    public static boolean versionGreaterOrEqualThen(String minimum) {
        Assert.notNull(minimum, "minimum must not be null");
        String tarantoolCiVersion = java.lang.System.getenv(TARANTOOL_VERSION);
        return versionGreaterOrEqualThen(tarantoolCiVersion, minimum);
    }

    public static boolean versionGreaterOrEqualThen(String current, String minimum) {
        return current == null || current.isEmpty() ||
               versionGreaterOrEqualThen(new TarantoolVersion(current), new TarantoolVersion(minimum));
    }

    public static boolean versionGreaterOrEqualThen(TarantoolVersion current, TarantoolVersion minimum) {
        return current.getMajor() > minimum.getMajor() ||
               current.getMajor().equals(minimum.getMajor()) && current.getMinor() >= minimum.getMinor();
    }

    public static boolean versionWithUUID() {
        return versionGreaterOrEqualThen("2.4");
    }

    public static boolean versionWithInstant() {
        return versionGreaterOrEqualThen("2.10");
    }

    public static boolean versionWithVarbinary() {
        return versionGreaterOrEqualThen("2.2.1");
    }

    public static class TarantoolVersion {
        private Integer major;
        private Integer minor;

        public TarantoolVersion(String stringVersion) {
            List<String> majorMinor = stringVersion == null || stringVersion.isEmpty() ?
                Collections.emptyList() :
                Arrays.stream(stringVersion.split("\\."))
                    .collect(Collectors.toList());
            if (majorMinor.size() >= 1) {
                major = Integer.parseInt(majorMinor.get(0));
            }
            if (majorMinor.size() >= 2) {
                String minorStr = majorMinor.get(1);
                minor = minorStr.equals("x") ? 999 : Integer.parseInt(minorStr);
            }
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
