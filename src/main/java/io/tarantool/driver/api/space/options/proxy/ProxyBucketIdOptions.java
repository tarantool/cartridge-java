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
     * Specifies bucket id for operation to perform it on storage with this bucket.
     * By default, crud extracts a priority key from a tuple, processes it through strcrc32 hash function
     * and gets remainder after dividing by the number of buckets.
     * You can use your own implementation of sharding function, but you don't forget about the number of buckets,
     * that you can get by {@code vshard.router.bucket_count} method.
     *
     * @param bucketId list of string field names
     * @return this options instance
     * @see <a href="https://github.com/tarantool/vshard">vshard</a>
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
