package com.g12.CPEN431.A12;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.g12.CPEN431.A12.codes.Command;
import com.g12.CPEN431.A12.codes.ErrorCodes;
import com.g12.CPEN431.A12.records.Address;
import com.g12.CPEN431.A12.records.KVStoreValue;
import com.g12.CPEN431.A12.utils.Utils;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicationService implements Runnable {
    private static final int WAIT_TIME_MILLIS = 2_000;
    private static final int STARTUP_DELAY_MILLIS = 2_000;
    private static final int BATCH_SIZE = 10;

    private final NodeStatus nodeStatus;
    private final ConcurrentHashMap<ByteString, KVStoreValue> keyValuesMap;
    private final ConcurrentHashMap<Integer, Set<ByteString>> replicaKeyMap;
    private Set<Integer> successors;
    private Set<Integer> deadPredecessors;

    public ReplicationService(
            NodeStatus nodeStatus,
            ConcurrentHashMap<ByteString, KVStoreValue> keyValuesMap,
            ConcurrentHashMap<Integer, Set<ByteString>> replicaKeyMap
    ) {
        this.nodeStatus = nodeStatus;
        this.keyValuesMap = keyValuesMap;
        this.replicaKeyMap = replicaKeyMap;
        this.successors = nodeStatus.getSuccessors();
        this.deadPredecessors = nodeStatus.getDeadPredecessors();
    }

    @Override
    public void run() {
        // start-up delay
        try {
            Thread.sleep(STARTUP_DELAY_MILLIS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        while (true) try {
            replicate();

            // interval delay
            try {
                Thread.sleep(WAIT_TIME_MILLIS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Checks for node failures and rejoins and replicates key values.
     */
    public void replicate() {
        // Primary node replicate from self and dead predecessors to new successors
        Set<Integer> currentSuccessors = nodeStatus.getSuccessors();
        Set<Integer> newSuccessors = new HashSet<>(currentSuccessors);
        newSuccessors.removeAll(successors);
        for (int node : newSuccessors) {
//            System.out.println("Me: " + nodeStatus.getMe() + " replication to node: " + node);
            sendKeyValuesToNode(nodeStatus.getMe(), node);
            for (int deadPredecessor : deadPredecessors) {
//                System.out.println("Me: " + nodeStatus.getMe() + " replication from node: " + deadPredecessor + " to node: " + node);
                sendKeyValuesToNode(deadPredecessor, node);
            }
        }
        successors = currentSuccessors;

        // Replicate new dead predecessors to successors
        Set<Integer> currentDeadPredecessors = nodeStatus.getDeadPredecessors();
        Set<Integer> newDeadPredecessors = new HashSet<>(currentDeadPredecessors);
        // TODO: this part can probably be optimized
        newDeadPredecessors.removeAll(deadPredecessors);
        for (int newDeadPredecessor : newDeadPredecessors) {
            for (int successor : successors) {
//                System.out.println("Me: " + nodeStatus.getMe() + " replication from node: " + newDeadPredecessor + " to node: " + successor);
                sendKeyValuesToNode(newDeadPredecessor, successor);
            }
        }

        // Replicate to rejoined predecessors
        Iterator<Integer> it = deadPredecessors.iterator();
        while (it.hasNext()) {
            int node = it.next();
            if (nodeStatus.isNodeAlive(node)) {
                for (int predecessor : nodeStatus.getDeadPredecessors(node)) {
//                    System.out.println("Me: " + nodeStatus.getMe() + " replication from node: " + predecessor + " to node: " + node);
                    sendKeyValuesToNode(predecessor, node);
                }
//                System.out.println("Me: " + nodeStatus.getMe() + " replication from node: " + node + " to node: " + node);
                sendKeyValuesToNode(node, node);
                it.remove();
            }
        }
        deadPredecessors = currentDeadPredecessors;
    }

    /**
     * Helper method to redistribute key-values from an origin node to a target node.
     *
     * @param originNode - origin node where the keys are mapped to
     * @param targetNode - The index of the target node.
     */
    public void sendKeyValuesToNode(int originNode, int targetNode) {
        if (!nodeStatus.isNodeAlive(targetNode)) {
            System.err.println("Target node " + targetNode + " is not alive");
            return;
        }

        // Get all keys from the origin node
        Set<ByteString> originKeySet = replicaKeyMap.get(originNode);
        if (originKeySet == null || originKeySet.isEmpty()) {
            return;
        }

        List<ByteString> originKeys = new ArrayList<>(originKeySet);

        new Thread(() -> {
            try {
                DatagramSocket socket = new DatagramSocket();
                int timeoutCount = 0;

                // Send Batch Put Request
                List<ByteString> batchKeys = new ArrayList<>();
                List<KVStoreValue> batchKVStoreValue = new ArrayList<>();
                int numBatches = originKeys.size() / BATCH_SIZE + 1;
                for (int i = 0; i < numBatches; i++) {
                    batchKeys.clear();
                    batchKVStoreValue.clear();
                    for (int j = 0; j < BATCH_SIZE; j++) {
                        int index = i * BATCH_SIZE + j;
                        if (index >= originKeys.size()) {
                            break;
                        }
                        ByteString key = originKeys.get(index);
                        // check if key exists in key-values map
                        if (keyValuesMap.containsKey(key)) {
                            batchKeys.add(key);
                            batchKVStoreValue.add(keyValuesMap.get(key));
                        }
                    }

                    // send batch put request to target node
                    try {
                        Message.Msg req = generateBatchPutRequest(batchKeys, batchKVStoreValue, socket);
                        Address targetAddress = nodeStatus.getAddress(targetNode);
                        KeyValueResponse.KVResponse payload = Utils.sendRequest(
                                socket,
                                req,
                                targetAddress.address(),
                                targetAddress.quorumPort()
                        );

                        // print error message if put request fails
                        if (payload == null) {
                            System.err.println("Inter-server put request timed out. (" +
                                    nodeStatus.getMe() + " -> " + targetNode + ")");
                            timeoutCount++;
                            if (timeoutCount >= 3) {
                                System.err.println("Abort sending key values to " + targetNode);
                                return;
                            }
                        } else if (payload.getErrCode() != ErrorCodes.SUCCESS) {
                            System.err.println("Inter-server put request fails: " + payload.getErrCode());
                        } else {
                            timeoutCount = 0;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (SocketException e) {
                e.printStackTrace();
            }
        }).start();
    }

    /**
     * Helper method to generate batch put requests for redistributing key-values.
     *
     * @param keys          - List of Keys meant for another node.
     * @param kvStoreValues - List of KVStoreValue meant for another node.
     * @return - The request Msg.
     * @throws IOException - This method may throw IOException.
     */
    private Message.Msg generateBatchPutRequest(
            List<ByteString> keys,
            List<KVStoreValue> kvStoreValues,
            DatagramSocket socket
    ) throws IOException {
        KeyValueRequest.KVRequest.Builder payloadBuilder = KeyValueRequest.KVRequest.newBuilder()
                .setCommand(Command.BATCH_PUT);

        int index = 0;
        for (KVStoreValue kvStoreValue : kvStoreValues) {
            KeyValueRequest.Put put = KeyValueRequest.Put.newBuilder()
                    .setKey(keys.get(index))
                    .setValue(kvStoreValue.value())
                    .setVersion(kvStoreValue.version())
                    .setTimestamp(kvStoreValue.timestamp())
                    .build();
            payloadBuilder.addBatchPut(put);
            index++;
        }

        KeyValueRequest.KVRequest payload = payloadBuilder.build();

        return Utils.constructRequestMsg(socket, payload);
    }
}
