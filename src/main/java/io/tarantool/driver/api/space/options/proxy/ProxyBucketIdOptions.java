package io.tarantool.driver.api.space.options.proxy;

import io.tarantool.driver.api.space.options.OperationWithBucketIdOptions;

import java.util.Optional;

/**
 * Represent options for functions that uses bucket id to find storage location.
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
abstract class ProxyBucketIdOptions<B extends ProxyBucketIdOptions<B>> extends ProxyBaseOptions<B>
        implements OperationWithBucketIdOptions {

    public static final String BUCKET_ID = "bucket_id";

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
    public B withBucketId(Integer bucketId) {
        addOption(BUCKET_ID, bucketId);
        return self();
    }

    @Override
    public Optional<Integer> getBucketId() {
        return getOption(BUCKET_ID, Integer.class);
    }
}
