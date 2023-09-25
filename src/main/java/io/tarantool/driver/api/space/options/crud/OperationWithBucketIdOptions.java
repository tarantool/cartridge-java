package io.tarantool.driver.api.space.options.crud;

import io.tarantool.driver.api.space.options.crud.enums.ProxyOption;
import io.tarantool.driver.api.space.options.Options;
import io.tarantool.driver.api.space.options.Self;

import java.util.Optional;

/**
 * Base interface for all operation options that may have a configurable bucket id.
 *
 * @author Artyom Dubinin
 */
public interface OperationWithBucketIdOptions<T extends OperationWithBucketIdOptions<T>>
    extends Options, Self<T> {

    /**
     * Specifies bucket id for an operation to perform it on storage with this bucket. It may be useful
     * if a non-default sharding function is used or in other specific cases.
     * By default, crud extracts primary key from a tuple and calculates a strcrc32 hash function value
     * and gets the remainder after dividing the value by the number of buckets.
     * You can use your own implementation of the sharding function. For retrieving the number of buckets
     * the {@code vshard.router.bucket_count} API method can be used.
     *
     * @param bucketId number determining the location in the cluster
     * @return this options instance
     * @see <a href="https://github.com/tarantool/vshard#sharding-architecture">vshard</a>
     * @see <a href="https://github.com/tarantool/crud">crud</a>
     */
    default T withBucketId(Integer bucketId) {
        addOption(ProxyOption.BUCKET_ID, bucketId);
        return self();
    }

    /**
     * Return bucket id that is used by operation to find storage location.
     *
     * @return bucket id
     */
    default Optional<Integer> getBucketId() {
        return getOption(ProxyOption.BUCKET_ID, Integer.class);
    }
}
