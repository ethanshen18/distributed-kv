package com.g12.CPEN431.A12.codes;

public final class Command {
    /**
     * Put: This is a put operation.
     */
    public static final int PUT = 1;
    /**
     * Get: This is a get operation.
     */
    public static final int GET = 2;
    /**
     * Remove: This is a remove operation.
     */
    public static final int REMOVE = 3;
    /**
     * Shutdown: shuts-down the node (used for testing and management).
     * The expected behaviour is that your implementation immediately calls System.exit().
     */
    public static final int SHUTDOWN = 4;
    /**
     * Wipeout: deletes all keys stored in the node (used for testing)
     */
    public static final int WIPEOUT = 5;
    /**
     * IsAlive: does nothing but replies with success if the node is alive.
     */
    public static final int IS_ALIVE = 6;
    /**
     * GetPID: the node is expected to reply with the processID of the Java process
     */
    public static final int GET_PID = 7;
    /**
     * GetMembershipCount: the node is expected to reply with the count of the currently active members based
     * on your membership protocol.
     */
    public static final int GET_MEMBERSHIP_COUNT = 8;
    /**
     * GetStatuses: the node is expected to reply with the internal status of all the nodes in the system.
     * The status contains the node index and the last-alive timestamp.
     */
    public static final int GET_STATUS = 9;
    /**
     * ChainPut: the node is expected to store the key-value pair and replicate to successor if needed.
     */
    public static final int CHAIN_PUT = 10;
    /**
     * ChainRemove: the node is expected to remove the key-value pair and replicate remove to successor if needed.
     */
    public static final int CHAIN_REMOVE = 11;
    /**
     * BatchPut: the node is expected to store the key-value pairs while checking for timestamp conflicts.
     */
    public static final int BATCH_PUT = 12;
    /**
     * QuorumGet: the node is expected to get the value and timestamp of the key.
     */
    public static final int QUORUM_GET = 13;

    public static boolean hasKey(int command) {
        return switch (command) {
            case PUT, GET, REMOVE -> true;
            default -> false;
        };
    }
}
