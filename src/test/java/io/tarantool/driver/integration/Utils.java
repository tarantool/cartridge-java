package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.protocol.Packable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Ivan Dneprov
 * @author Artyom Dubinin
 */
public final class Utils {
    private static Optional<Integer> bucketCount = Optional.empty();

    private Utils() {
    }

    /**
     * Checks if the space is empty.
     *
     * @param testSpace space to check
     */
    static void checkSpaceIsEmpty(TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> testSpace) {
        assertEquals(0, testSpace.select(Conditions.any()).thenApply(List::size).join());
    }

    /**
     * Get number of buckets in vshard cluster.
     *
     * @param client Tarantool client for with access to vshard router
     * @param <T>    target tuple type
     * @param <R>    target tuple collection type
     * @return number of buckets
     */
    public static <T extends Packable, R extends Collection<T>> Integer getBucketCount(
        TarantoolClient<T, R> client) throws ExecutionException, InterruptedException {
        if (!bucketCount.isPresent()) {
            bucketCount = Optional.ofNullable(
                client.callForSingleResult("vshard.router.bucket_count", Integer.class).get()
            );
        }
        return bucketCount.orElseThrow(() -> new TarantoolClientException("Failed to get bucket count"));
    }

    /**
     * Get bucket_id via crc32 hash function.
     * You can't use null, because null is packed to box.NULL((void *) 0) and java doesn't have equivalent.
     *
     * @param client Tarantool client for with access to vshard router
     * @param key    key that will be used to calculate bucketId
     * @param <T>    target tuple type
     * @param <R>    target tuple collection type
     * @return bucketId number determining the location in the cluster
     */
    public static <T extends Packable, R extends Collection<T>> Integer getBucketIdStrCRC32(
        TarantoolClient<T, R> client, List<Object> key) throws ExecutionException, InterruptedException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (Object part : key) {
            try {
                if (part != null) {
                    outputStream.write(part.toString().getBytes());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return Math.toIntExact(
            (crc32(outputStream.toByteArray()) % getBucketCount(client)) + 1
        );
    }

    /**
     * Implementation of crc32 partially was taken from
     * <a href="https://github.com/TheAlgorithms/Java/blob/master/src/main/java/com/thealgorithms/others/CRC32.java">
     * github.com/TheAlgorithms</a>
     *
     * @param data input bytes array
     * @return hash response in decimal view
     */
    private static long crc32(byte[] data) {
        BitSet bitSet = BitSet.valueOf(data);
        int crc32 = 0xFFFFFFFF; // initial value
        for (int i = 0; i < data.length * 8; i++) {
            if (((crc32 >>> 31) & 1) != (bitSet.get(i) ? 1 : 0)) {
                crc32 = (crc32 << 1) ^ 0x1EDC6F41; // xor with polynomial
            } else {
                crc32 = crc32 << 1;
            }
        }
        crc32 = Integer.reverse(crc32); // result reflect
        return crc32 & 0x00000000ffffffffL; // the unsigned java problem
    }

    /**
     * Converts a byte array to a list of bytes
     *
     * @param bytes byte array
     */
    static List<Byte> convertBytesToByteList(byte[] bytes) {
        return IntStream.range(0, bytes.length).mapToObj(i -> bytes[i]).collect(Collectors.toList());
    }
}
