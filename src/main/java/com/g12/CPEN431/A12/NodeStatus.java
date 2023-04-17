package com.g12.CPEN431.A12;

import com.g12.CPEN431.A12.records.Address;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.g12.CPEN431.A12.utils.HashUtils.NUM_BUCKETS;

public class NodeStatus {
    private static final int IS_ALIVE_THRESHOLD_MS = 14_000;
    private final ArrayList<Address> addresses;
    private final List<Long> lastAliveTimestamps;
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final int me;

    public NodeStatus(List<Address> addresses, Address myAddress) {
        this.addresses = new ArrayList<>(addresses);
        long currTimestamp = System.currentTimeMillis();

        try {
            this.rwLock.writeLock().lock();
            this.lastAliveTimestamps = new ArrayList<>(Collections.nCopies(addresses.size(), currTimestamp));
        } finally {
            this.rwLock.writeLock().unlock();
        }

        this.me = this.addresses.indexOf(myAddress);
        if (this.me == -1) {
            throw new RuntimeException("List of addresses does not contain my address");
        }
    }

    public int getMe() {
        return me;
    }

    public Address getAddress(int i) {
        return addresses.get(i);
    }

    public boolean isTimestampAlive(long timestamp) {
        return (System.currentTimeMillis() - timestamp) < IS_ALIVE_THRESHOLD_MS;
    }

    public boolean isNodeAlive(int i) {
        updateSelfTimestamp();

        try {
            rwLock.readLock().lock();
            return isTimestampAlive(lastAliveTimestamps.get(i));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void updateSelfTimestamp() {
        try {
            rwLock.writeLock().lock();
            Long currentTimeMillis = System.currentTimeMillis();
            lastAliveTimestamps.set(me, currentTimeMillis);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void updateLastAliveTimestamp(int i, long newTimestamp) {
        try {
            rwLock.writeLock().lock();

            // update if newer
            if (newTimestamp > lastAliveTimestamps.get(i)) {
                lastAliveTimestamps.set(i, newTimestamp);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void updateLastAliveTimestamps(List<Long> newTimestamps) {
        updateSelfTimestamp();

        for (int i = 0; i < NUM_BUCKETS; i++) {
            updateLastAliveTimestamp(i, newTimestamps.get(i));
        }
    }

    public List<Long> getLastAliveTimestamps() {
        updateSelfTimestamp();

        try {
            rwLock.readLock().lock();
            return List.copyOf(lastAliveTimestamps);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public int getNumAlive() {
        updateSelfTimestamp();
        int numAlive = 0;

        try {
            rwLock.readLock().lock();
            for (long timestamp : lastAliveTimestamps) {
                if (isTimestampAlive(timestamp)) numAlive++;
            }
        } finally {
            rwLock.readLock().unlock();
        }

        return numAlive;
    }

    /**
     * Get alive predecessors of self node
     *
     * @return set of alive predecessor nodes of self node
     */
    public HashSet<Integer> getPredecessors() {
        return getPredecessors(me);
    }

    /**
     * Get alive predecessors of a node
     *
     * @param node reference node
     * @return set of alive predecessor nodes of the reference node
     */
    public HashSet<Integer> getPredecessors(int node) {
        HashSet<Integer> predecessors = new HashSet<>();
        int seenAliveNodes = 0;

        // Loop through replica interval
        while (seenAliveNodes < 3) {
            node = ((node - 1) + NUM_BUCKETS) % NUM_BUCKETS;
            if (isNodeAlive(node)) {
                seenAliveNodes++;
                predecessors.add(node);
            }
        }

        return predecessors;
    }

    /**
     * Get successor of self node
     *
     * @return the successor of self node
     */
    public int getSuccessor() {
        return getSuccessor(me);
    }

    /**
     * Get successor of a node
     *
     * @param node reference node
     * @return the successor of the reference node
     */
    public int getSuccessor(int node) {
        do {
            node = (node + 1) % NUM_BUCKETS;
        } while (!isNodeAlive(node) && node != me);
        return node;
    }

    /**
     * Get last (3rd) successor of a node
     *
     * @param node reference node
     * @return the last successor of the reference node
     */
    public int getLastSuccessor(int node) {
        int seenAliveNodes = 0;

        // Loop through replica interval
        while (seenAliveNodes < 3) {
            node = (node + 1) % NUM_BUCKETS;
            if (isNodeAlive(node)) {
                seenAliveNodes++;
            }
        }

        return node;
    }

    /**
     * Get successors of current node
     *
     * @return set of <b>3 alive nodes</b> that succeed the reference node.
     * Note that this includes <b>ONLY</b> alive nodes.
     */
    public HashSet<Integer> getSuccessors() {
        return getSuccessors(me);
    }

    /**
     * Get successors of a node
     *
     * @param node reference node
     * @return set of <b>3 alive nodes</b> that succeed the reference node.
     * Note that this includes <b>ONLY</b> alive nodes.
     */
    public HashSet<Integer> getSuccessors(int node) {
        HashSet<Integer> successors = new HashSet<>();

        int seenAliveNodes = 0;

        // Loop through replica interval
        while (seenAliveNodes < 3) {
            node = (node + 1) % NUM_BUCKETS;
            if (isNodeAlive(node)) {
                seenAliveNodes++;
                successors.add(node);
            }
        }

        return successors;
    }

    /**
     * Gets the closest alive node of the original node including the original node
     *
     * @param originalNode The original node
     * @return The closest alive node
     */
    public int getClosestAliveNode(int originalNode) {
        int node = originalNode;
        do {
            if (isNodeAlive(node)) {
                return node;
            }
            node = (node + 1) % NUM_BUCKETS;
        } while (node != originalNode);

        // Will not happen
        System.err.println("Could not find valid node to route request to");
        return originalNode;
    }

    /**
     * Get the dead predecessors of me
     *
     * @return The dead predecessors
     */
    public HashSet<Integer> getDeadPredecessors() {
        return getDeadPredecessors(me);
    }

    /**
     * Gets the dead predecessors of node, not including the original node
     *
     * @return The dead predecessors
     */
    public HashSet<Integer> getDeadPredecessors(int node) {
        node = ((node - 1) + NUM_BUCKETS) % NUM_BUCKETS;
        HashSet<Integer> deadPredecessors = new HashSet<>();
        while (!isNodeAlive(node)) {
            deadPredecessors.add(node);
            node = ((node - 1) + NUM_BUCKETS) % NUM_BUCKETS;
        }

        return deadPredecessors;
    }
}
