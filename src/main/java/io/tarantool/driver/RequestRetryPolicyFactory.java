package io.tarantool.driver;

/**
 * Manages instantiation of request retry policies. A policy contains an algorithm of deciding whether an exception
 * is retriable and settings for limiting the retry attempts
 *
 * @see RequestRetryPolicy
 * @author Alexey Kuzin
 */
public interface RequestRetryPolicyFactory {
    /**
     * Instantiate a new request retry policy instance. The policy may be either stateful or stateless, so depending on
     * that the policy may be either instantiated as a singleton or once per request.
     *
     * @return new policy instance
     */
    RequestRetryPolicy create();
}
