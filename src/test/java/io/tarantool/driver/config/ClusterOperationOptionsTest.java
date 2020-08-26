package io.tarantool.driver.config;

import io.tarantool.driver.cluster.ClusterOperationOptions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Sergey Volgin
 */
public class ClusterOperationOptionsTest {

    @Test
    public void createEmptyTest() {
        ClusterOperationOptions options = ClusterOperationOptions.builder().build();
        assertEquals(Collections.EMPTY_MAP, options.asMap());
    }

    @Test
    public void createNotEmptyTest() {
        ClusterOperationOptions options = ClusterOperationOptions.builder()
                .withTimeout(1000)
                .withTuplesAsMap(true)
                .withSelectKey(Arrays.asList(1, "a"))
                .withSelectLimit(50)
                .withSelectOffset(100)
                .withSelectBatchSize(10)
                .withSelectIterator("GT")
                .build();

        assertEquals(7, options.asMap().size());

        assertEquals(1000, options.asMap().get(ClusterOperationOptions.TIME_OUT));
        assertTrue((Boolean) options.asMap().get(ClusterOperationOptions.TUPLES_AS_MAP));
        assertEquals(Arrays.asList(1, "a"), options.asMap().get(ClusterOperationOptions.SELECT_KEY));
        assertEquals(50L, options.asMap().get(ClusterOperationOptions.SELECT_LIMIT));
        assertEquals(100L, options.asMap().get(ClusterOperationOptions.SELECT_OFFSET));
        assertEquals(10L, options.asMap().get(ClusterOperationOptions.SELECT_BATCH_SIZE));
        assertEquals("GT", options.asMap().get(ClusterOperationOptions.SELECT_ITERATOR));
    }
}
