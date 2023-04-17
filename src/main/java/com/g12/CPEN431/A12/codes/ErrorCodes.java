package com.g12.CPEN431.A12.codes;

public final class ErrorCodes {
    /**
     * Operation is successful.
     */
    public static final int SUCCESS = 0;
    /**
     * Non-existent key requested in a get or delete operation
     */
    public static final int NON_EXISTENT_KEY = 1;
    /**
     * Out-of-space.  Returned when there is no space left to store data for an additional PUT.
     * Operations that do not consume new space (e.g., GET, REMOVE) would generally not
     * (or ‘never’ if you can guarantee it) return this error code.
     */
    public static final int OUT_OF_SPACE = 2;
    /**
     * Temporary system overload. The system is operational but decides to refuse the operation due to
     * temporary overload that consumes internal resources (e.g., full internal buffers, too many in-flight requests).
     * Otherwise, said, this error code indicates that, if nothing happens and no new requests are accepted for a while,
     * the system will likely to return to a functioning state. This is a signal so that a well behaved client will
     * wait for some overloadWaitTime (in milliseconds) and either continue or retry the same operation.
     */
    public static final int TEMPORARY_SYSTEM_OVERLOAD = 3;
    /**
     * Internal KVStore failure - a catch-all for all other situations where your KVStore has determined that
     * something is wrong and it can not recover from the failure.
     */
    public static final int INTERNAL_KV_STORE_FAILURE = 4;
    /**
     * Unrecognized command.
     */
    public static final int UNRECOGNIZED_COMMAND = 5;
    /**
     * Invalid key:  the key length is invalid (e.g., greater than the maximum allowed length).
     */
    public static final int INVALID_KEY = 6;
    /**
     * Invalid value: the value length is invalid (e.g., greater than the maximum allowed length).
     */
    public static final int INVALID_VALUE = 7;
}
