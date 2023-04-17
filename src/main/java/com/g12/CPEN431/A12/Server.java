package com.g12.CPEN431.A12;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest.KVRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueRequest.Put;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;
import ca.NetSysLab.ProtocolBuffers.Message.Msg;
import com.g12.CPEN431.A12.codes.Command;
import com.g12.CPEN431.A12.codes.ErrorCodes;
import com.g12.CPEN431.A12.records.Address;
import com.g12.CPEN431.A12.records.KVStoreValue;
import com.g12.CPEN431.A12.records.Request;
import com.g12.CPEN431.A12.utils.HashUtils;
import com.g12.CPEN431.A12.utils.Utils;
import com.google.common.cache.Cache;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.g12.CPEN431.A12.utils.HashUtils.NUM_BUCKETS;

public class Server implements Runnable {
    private final DatagramSocket socket;
    private final DatagramSocket replicaSocket;
    private final BlockingQueue<Request> requestQueue;
    private final ConcurrentHashMap<ByteString, KVStoreValue> keyValuesMap;
    private final ConcurrentHashMap<Integer, Set<ByteString>> replicaKeyMap;
    private final Cache<ByteString, ByteString> cache;
    private final NodeStatus nodeStatus;
    private final int pid;
    private final Runtime runtime;
    private final ExecutorService executorService = Executors.newFixedThreadPool(3);
    private final SocketPool socketPool = new SocketPool(6);

    public Server(
            Cache<ByteString, ByteString> cache,
            BlockingQueue<Request> requestQueue,
            ConcurrentHashMap<ByteString, KVStoreValue> keyValuesMap,
            ConcurrentHashMap<Integer, Set<ByteString>> replicaKeyMap,
            NodeStatus nodeStatus,
            Runtime runtime,
            int pid
    ) throws SocketException {
        this.socket = new DatagramSocket();
        this.replicaSocket = new DatagramSocket();
        this.requestQueue = requestQueue;
        this.keyValuesMap = keyValuesMap;
        this.replicaKeyMap = replicaKeyMap;
        this.runtime = runtime;
        this.cache = cache;
        this.nodeStatus = nodeStatus;
        this.pid = pid;
    }

    @Override
    public void run() {
        while (true) try {
            Request request = requestQueue.take();
            handleRequest(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleRequest(Request request) throws NullPointerException, IOException {
        KVRequest requestPayload = request.payload();
        int command = requestPayload.getCommand();
        KVResponse response = null;

        switch (command) {
            case Command.PUT -> {

                // Check hashmap and JVM capacity
                if (runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory()) < 2_000_000) {
                    response = KVResponse.newBuilder().setErrCode(ErrorCodes.OUT_OF_SPACE).build();
                    break;
                }

                // Get key-value from request
                ByteString key = requestPayload.getKey();
                byte[] keyBytes = key.toByteArray();
                ByteString value = requestPayload.getValue();
                byte[] valueBytes = value.toByteArray();

                // Check valid key
                if (keyBytes == null || keyBytes.length == 0 || keyBytes.length > 32) {
                    response = KVResponse.newBuilder().setErrCode(ErrorCodes.INVALID_KEY).build();
                    break;
                }

                // Check valid value
                if (valueBytes == null || valueBytes.length > 10_000) {
                    response = KVResponse.newBuilder().setErrCode(ErrorCodes.INVALID_VALUE).build();
                    break;
                }

                // Add key-value to keyValuesMap
                int version = requestPayload.hasVersion() ? requestPayload.getVersion() : 0;
                long currentTimestamp = System.currentTimeMillis();
                keyValuesMap.put(key, new KVStoreValue(value, version, currentTimestamp));

                // Add key-value to replicaKeyMap
                int originalBucket = HashUtils.getBucketFromKey(keyBytes);
                updateReplicaKeyMap(key, originalBucket);

                if (NUM_BUCKETS > 1) {
                    // Do chain replication
                    Msg chainPutRequest = generateChainPutRequest(key,
                            value,
                            version,
                            currentTimestamp,
                            3,
                            request);
                    Address targetAddress = nodeStatus.getAddress(nodeStatus.getSuccessor());
                    Utils.sendRequestNoWait(
                            replicaSocket,
                            chainPutRequest,
                            targetAddress.address(),
                            targetAddress.quorumPort()
                    );
                    return;
                }

                // Build response
                response = KVResponse.newBuilder().setErrCode(ErrorCodes.SUCCESS).build();
            }

            case Command.GET -> {
                KVStoreValue value = keyValuesMap.get(requestPayload.getKey());
                if (value == null) {
                    if (NUM_BUCKETS > 1) {
                        KVResponse quorumGetResponse = performQuorumGet(requestPayload.getKey());
                        if (quorumGetResponse == null) {
                            response = KVResponse.newBuilder().setErrCode(ErrorCodes.NON_EXISTENT_KEY).build();
                        } else {
                            response = KVResponse.newBuilder()
                                    .setErrCode(ErrorCodes.SUCCESS)
                                    .setValue(quorumGetResponse.getValue())
                                    .setVersion(quorumGetResponse.getVersion())
                                    .build();
                        }
                    } else {
                        response = KVResponse.newBuilder().setErrCode(ErrorCodes.NON_EXISTENT_KEY).build();
                    }
                } else {
                    response = KVResponse.newBuilder()
                            .setErrCode(ErrorCodes.SUCCESS)
                            .setValue(value.value())
                            .setVersion(value.version())
                            .build();
                }
            }

            case Command.REMOVE -> {

                // Get key from request
                ByteString key = requestPayload.getKey();
                byte[] keyBytes = key.toByteArray();
                if (keyBytes == null || keyBytes.length == 0 || keyBytes.length > 32) {
                    response = KVResponse.newBuilder().setErrCode(ErrorCodes.INVALID_KEY).build();
                    break;
                }

                // Attempt to remove the key-value
                KVStoreValue value = keyValuesMap.remove(key);
                if (value != null) {
                    // Remove key-values from replicaKeyMap
                    int originalBucket = HashUtils.getBucketFromKey(keyBytes);
                    Set<ByteString> keys = replicaKeyMap.get(originalBucket);
                    if (keys != null) {
                        keys.remove(key);
                    }
                }

                if (NUM_BUCKETS > 1) {
                    // Do chain remove
                    Msg chainRemoveRequest = generateChainRemoveRequest(key,
                            3,
                            request);
                    Address targetAddress = nodeStatus.getAddress(nodeStatus.getSuccessor());
                    Utils.sendRequestNoWait(
                            replicaSocket,
                            chainRemoveRequest,
                            targetAddress.address(),
                            targetAddress.quorumPort()
                    );
                    return;
                }

                if (value == null) {
                    response = KVResponse.newBuilder().setErrCode(ErrorCodes.NON_EXISTENT_KEY).build();
                } else {
                    response = KVResponse.newBuilder().setErrCode(ErrorCodes.SUCCESS).build();
                }
            }

            case Command.SHUTDOWN -> System.exit(0);

            case Command.WIPEOUT -> {
                keyValuesMap.clear();
                replicaKeyMap.clear();
                System.gc();
                response = KVResponse.newBuilder().setErrCode(ErrorCodes.SUCCESS).build();
            }

            case Command.IS_ALIVE -> response = KVResponse.newBuilder()
                    .setErrCode(ErrorCodes.SUCCESS)
                    .build();

            case Command.GET_PID -> {
                System.out.println("Me: " + nodeStatus.getMe());
                for (Map.Entry<Integer, Set<ByteString>> entry : replicaKeyMap.entrySet()) {
                    System.out.println("For node: " + entry.getKey() + ", has key values:");
                    for (ByteString key : entry.getValue()) {
                        String keyString = new String(key.toByteArray());
                        String valueString = new String(keyValuesMap.get(key).value().toByteArray());
                        System.out.println("    {" + keyString + ":" + valueString + "}");
                    }
                }

                response = KVResponse.newBuilder()
                        .setErrCode(ErrorCodes.SUCCESS)
                        .setPid(pid)
                        .build();
            }

            case Command.GET_MEMBERSHIP_COUNT -> response = KVResponse.newBuilder()
                    .setErrCode(ErrorCodes.SUCCESS)
                    .setMembershipCount(nodeStatus.getNumAlive())
                    .build();

            case Command.GET_STATUS -> {

                // Verify and save timestamps from requesting node
                List<Long> newTimestamps = requestPayload.getLastAliveTimestampsList();
                if (requestPayload.getLastAliveTimestampsCount() == NUM_BUCKETS) {
                    nodeStatus.updateLastAliveTimestamps(newTimestamps);
                } else if (requestPayload.getLastAliveTimestampsCount() == 0) {
                    System.out.println("Received empty timestamps array");
                } else {
                    System.out.println("Received timestamps array length mismatch: " + requestPayload.getLastAliveTimestampsCount());
                }

                // Send back internal last-alive timestamps
                response = KVResponse.newBuilder()
                        .setErrCode(ErrorCodes.SUCCESS)
                        .addAllLastAliveTimestamps(nodeStatus.getLastAliveTimestamps())
                        .build();
            }

            case Command.CHAIN_PUT -> {
                // Get key-value from request
                ByteString key = requestPayload.getKey();
                ByteString value = requestPayload.getValue();
                int version = requestPayload.getVersion();
                long timestamp = requestPayload.getKeyValueTimestamp();

                keyValuesMap.put(key, new KVStoreValue(value, version, timestamp));

                // Add key-values to replicaKeyMap
                int originalBucket = HashUtils.getBucketFromKey(key.toByteArray());
                updateReplicaKeyMap(key, originalBucket);

                int chainCount = requestPayload.getChainCount() - 1;
                if (chainCount != 0) {
                    // Do chain replication
                    Msg chainPutRequest = generateChainPutRequest(key,
                            value,
                            version,
                            timestamp,
                            chainCount,
                            request);
                    Address targetAddress = nodeStatus.getAddress(nodeStatus.getSuccessor());
                    Utils.sendRequestNoWait(
                            replicaSocket,
                            chainPutRequest,
                            targetAddress.address(),
                            targetAddress.quorumPort()
                    );
                    return;
                }

                response = KVResponse.newBuilder().setErrCode(ErrorCodes.SUCCESS).build();
            }

            case Command.CHAIN_REMOVE -> {
                // Get key from request
                ByteString key = requestPayload.getKey();

                // Attempt to remove the key-value
                KVStoreValue value = keyValuesMap.remove(key);
                if (value != null) {
                    // Remove key-values from replicaKeyMap
                    int originalBucket = HashUtils.getBucketFromKey(key.toByteArray());
                    Set<ByteString> keys = replicaKeyMap.get(originalBucket);
                    if (keys != null) {
                        keys.remove(key);
                    }
                }

                int chainCount = requestPayload.getChainCount() - 1;
                if (chainCount != 0) {
                    // Do chain remove
                    Msg chainRemoveRequest = generateChainRemoveRequest(key,
                            chainCount,
                            request);
                    Address targetAddress = nodeStatus.getAddress(nodeStatus.getSuccessor());
                    Utils.sendRequestNoWait(
                            replicaSocket,
                            chainRemoveRequest,
                            targetAddress.address(),
                            targetAddress.quorumPort()
                    );
                    return;
                }

                if (value == null) {
                    response = KVResponse.newBuilder().setErrCode(ErrorCodes.NON_EXISTENT_KEY).build();
                } else {
                    response = KVResponse.newBuilder().setErrCode(ErrorCodes.SUCCESS).build();
                }
            }

            case Command.BATCH_PUT -> {
                // Get key-value from request
                List<Put> batchPuts = requestPayload.getBatchPutList();

                for (Put put : batchPuts) {
                    ByteString key = put.getKey();
                    ByteString value = put.getValue();
                    int version = put.getVersion();
                    long timestamp = put.getTimestamp();

                    // Resolve timestamp conflict
                    KVStoreValue existingKVStoreValue = keyValuesMap.get(key);
                    if (existingKVStoreValue == null || existingKVStoreValue.timestamp() < timestamp) {
                        // Update key-values in keyValuesMap
                        keyValuesMap.put(key, new KVStoreValue(value, version, timestamp));

                        // Add key-values to replicaKeyMap
                        int originalBucket = HashUtils.getBucketFromKey(key.toByteArray());
                        updateReplicaKeyMap(key, originalBucket);
                    } else {
                        if (existingKVStoreValue.timestamp() > timestamp) {
                            System.out.println("Received replication put with older timestamp");
                        }
                    }
                }
                response = KVResponse.newBuilder().setErrCode(ErrorCodes.SUCCESS).build();
            }

            case Command.QUORUM_GET -> {
                KVStoreValue value = keyValuesMap.get(requestPayload.getKey());
                if (value == null) {
                    response = KVResponse.newBuilder().setErrCode(ErrorCodes.NON_EXISTENT_KEY).build();
                } else {
                    response = KVResponse.newBuilder()
                            .setErrCode(ErrorCodes.SUCCESS)
                            .setValue(value.value())
                            .setVersion(value.version())
                            .setKeyValueTimestamp(value.timestamp())
                            .build();
                }
            }

            default -> response = KVResponse.newBuilder()
                    .setErrCode(ErrorCodes.UNRECOGNIZED_COMMAND)
                    .setMembershipCount(1)
                    .build();
        }
        sendResponse(response, request);
    }

    private void updateReplicaKeyMap(ByteString key, int originalBucket) {
        Set<ByteString> keys = replicaKeyMap.getOrDefault(originalBucket, ConcurrentHashMap.newKeySet());
        keys.add(key);
        replicaKeyMap.put(originalBucket, keys);
    }

    private Msg cacheResponseMessage(KVResponse response, Request request) throws IOException {
        long checkSum = Utils.getCheckSum(request.messageID().toByteArray(), response.toByteArray());
        Msg msg = Msg.newBuilder()
                .setMessageID(request.messageID())
                .setPayload(response.toByteString())
                .setCheckSum(checkSum)
                .build();
        cache.put(request.messageID(), msg.toByteString());
        return msg;
    }

    private void sendResponse(KVResponse response, Request request) throws IOException {
        Msg msg = cacheResponseMessage(response, request);
        DatagramPacket packet = request.packet();
        packet.setData(msg.toByteArray());
        socket.send(packet);
    }

    /**
     * Helper method to generate chain put request
     *
     * @param key             The key of the request
     * @param value           The value of the request
     * @param version         The version of the request
     * @param timestamp       The timestamp of the request
     * @param chainCount      The number of remaining chain put requests to send
     * @param originalRequest The request to get the client address, port, and message ID from
     * @return The request Msg
     * @throws IOException This method may throw IOException.
     */
    private Msg generateChainPutRequest(ByteString key,
                                        ByteString value,
                                        int version,
                                        long timestamp,
                                        int chainCount,
                                        Request originalRequest) throws IOException {
        String clientAddress = originalRequest.packet().getAddress().getHostAddress();
        int clientPort = originalRequest.packet().getPort();
        ByteString messageID = originalRequest.messageID();

        KVRequest payload = KVRequest.newBuilder()
                .setCommand(Command.CHAIN_PUT)
                .setKey(key)
                .setValue(value)
                .setVersion(version)
                .setKeyValueTimestamp(timestamp)
                .setChainCount(chainCount)
                .setClientAddress(clientAddress)
                .setClientPort(clientPort)
                .build();
        long checksum = Utils.getCheckSum(messageID.toByteArray(), payload.toByteArray());
        return Msg.newBuilder()
                .setMessageID(messageID)
                .setPayload(payload.toByteString())
                .setCheckSum(checksum)
                .build();
    }

    /**
     * Helper method to generate remove request for all replicas
     *
     * @param key             The key of the write quorum request
     * @param chainCount      The number of remaining chain remove requests to send
     * @param originalRequest The request to get the client address, port, and message ID from
     * @return The request Msg
     * @throws IOException This method may throw IOException.
     */
    private Msg generateChainRemoveRequest(ByteString key,
                                           int chainCount,
                                           Request originalRequest) throws IOException {
        String clientAddress = originalRequest.packet().getAddress().getHostAddress();
        int clientPort = originalRequest.packet().getPort();
        ByteString messageID = originalRequest.messageID();

        KVRequest payload = KVRequest.newBuilder()
                .setCommand(Command.CHAIN_REMOVE)
                .setKey(key)
                .setChainCount(chainCount)
                .setClientAddress(clientAddress)
                .setClientPort(clientPort)
                .build();
        long checksum = Utils.getCheckSum(messageID.toByteArray(), payload.toByteArray());
        return Msg.newBuilder()
                .setMessageID(messageID)
                .setPayload(payload.toByteString())
                .setCheckSum(checksum)
                .build();
    }

    /**
     * Helper method to perform quorum get requests
     *
     * @param key The key of the request
     * @return The response with the highest timestamp. Null if no successors have key
     */
    private KVResponse performQuorumGet(ByteString key) {
        Set<Integer> predecessors = nodeStatus.getPredecessors();
        List<CompletableFuture<KVResponse>> futures = new ArrayList<>();
        for (int predecessor : predecessors) {
            CompletableFuture<KVResponse> future = CompletableFuture.supplyAsync(() -> {
                DatagramSocket executorSocket = socketPool.getSocket();
                try {
                    Msg quorumGetRequest = generateQuorumGetRequest(key, executorSocket);
                    Address targetAddress = nodeStatus.getAddress(predecessor);
                    KVResponse response = Utils.sendRequest(
                            executorSocket,
                            quorumGetRequest,
                            targetAddress.address(),
                            targetAddress.quorumPort()
                    );
                    if (response != null && response.getErrCode() == ErrorCodes.SUCCESS) {
                        return response;
                    } else {
                        return null;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                } finally {
                    socketPool.returnSocket(executorSocket);
                }
            }, executorService);
            futures.add(future);
        }
        List<KVResponse> responses = futures.stream().map(CompletableFuture::join).toList();
        long highestTimestamp = 0;
        KVResponse response = null;
        for (KVResponse responsePayload : responses) {
            if (responsePayload == null) {
                continue;
            }
            if (responsePayload.getKeyValueTimestamp() > highestTimestamp) {
                highestTimestamp = responsePayload.getKeyValueTimestamp();
                response = responsePayload;
            }
        }
        return response;
    }

    /**
     * Helper method to generate quorum get request
     *
     * @param key    The key of the request
     * @param socket The socket to construct the message ID with
     * @return The request Msg
     * @throws IOException This method may throw IOException.
     */
    private Msg generateQuorumGetRequest(ByteString key, DatagramSocket socket) throws IOException {
        KVRequest payload = KVRequest.newBuilder()
                .setCommand(Command.QUORUM_GET)
                .setKey(key)
                .build();

        return Utils.constructRequestMsg(socket, payload);
    }
}
