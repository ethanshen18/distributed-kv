package com.g12.CPEN431.A12;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest.KVRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;
import ca.NetSysLab.ProtocolBuffers.Message.Msg;
import com.g12.CPEN431.A12.codes.Command;
import com.g12.CPEN431.A12.codes.ErrorCodes;
import com.g12.CPEN431.A12.records.Address;
import com.g12.CPEN431.A12.records.Request;
import com.g12.CPEN431.A12.utils.HashUtils;
import com.g12.CPEN431.A12.utils.Utils;
import com.google.common.cache.Cache;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class LoadBalancer implements Runnable {
    private final DatagramSocket socket;
    private final DatagramSocket routeSocket;
    private final NodeStatus nodeStatus;
    private final BlockingQueue<Request> requestQueue;
    private final Cache<ByteString, ByteString> cache;

    public LoadBalancer(
            DatagramSocket socket,
            DatagramSocket routeSocket,
            NodeStatus nodeStatus,
            BlockingQueue<Request> requestQueue,
            Cache<ByteString, ByteString> cache
    ) {
        this.socket = socket;
        this.routeSocket = routeSocket;
        this.nodeStatus = nodeStatus;
        this.requestQueue = requestQueue;
        this.cache = cache;
    }

    @Override
    public void run() {
        while (true) try {
            byte[] buf = new byte[16_000];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            socket.receive(packet);
            Msg msg = Msg.parseFrom(Arrays.copyOf(packet.getData(), packet.getLength()));

            // Extract values
            byte[] messageID = msg.getMessageID().toByteArray();
            byte[] payload = msg.getPayload().toByteArray();

            // Checksum
            long requestChecksum = Utils.getCheckSum(messageID, payload);
            if (requestChecksum != msg.getCheckSum()) {
                System.out.print("Invalid checksum; ");
                continue;
            }

            KVRequest requestPayload = KVRequest.parseFrom(payload);

            // Check cache
            ByteString cachedResponse = cache.getIfPresent(msg.getMessageID());
            if (cachedResponse != null) {
                if (requestPayload.hasClientAddress()) {
                    String clientAddress = requestPayload.getClientAddress();
                    int clientPort = requestPayload.getClientPort();
                    packet.setAddress(InetAddress.getByName(clientAddress));
                    packet.setPort(clientPort);
                }
                sendResponse(cachedResponse.toByteArray(), packet);
                continue;
            }
            if (cache.size() == 70_000) {
                sendOverloadResponse(msg.getMessageID(), packet);
                continue;
            }

            // Check if request is routed from another node
            if (requestPayload.hasClientAddress()) {
                String clientAddress = requestPayload.getClientAddress();
                int clientPort = requestPayload.getClientPort();
                packet.setAddress(InetAddress.getByName(clientAddress));
                packet.setPort(clientPort);
                offerToRequestQueue(requestPayload, msg.getMessageID(), packet);
                continue;
            }

            // Route request
            if (Command.hasKey(requestPayload.getCommand())) {
                byte[] keyBytes = requestPayload.getKey().toByteArray();

                int bucket = HashUtils.getBucketFromKey(keyBytes);
//                System.out.println("Me: " + nodeStatus.getMe() + " original bucket: " + bucket);
                if (bucket != nodeStatus.getMe()) {
                    bucket = nodeStatus.getClosestAliveNode(bucket);
                    if (bucket != nodeStatus.getMe()) {
                        // Route request to bucket
                        Address address;
                        if (requestPayload.getCommand() == Command.GET) {
//                            System.out.println("Me: " + nodeStatus.getMe() + " getting routed to bucket: " + nodeStatus.getLastSuccessor(bucket));
                            address = nodeStatus.getAddress(nodeStatus.getLastSuccessor(bucket));
                        } else {
//                            System.out.println("Me: " + nodeStatus.getMe() + " getting routed to bucket: " + bucket);
                            address = nodeStatus.getAddress(bucket);
                        }
                        KVRequest routeRequest = KVRequest.newBuilder(requestPayload)
                                .setClientAddress(packet.getAddress().getHostAddress())
                                .setClientPort(packet.getPort())
                                .build();
                        sendRouteRequest(
                                msg.getMessageID(),
                                routeRequest,
                                address.address(),
                                address.port()
                        );
                        continue;
                    }
                }
            }

            offerToRequestQueue(requestPayload, msg.getMessageID(), packet);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void offerToRequestQueue(KVRequest requestPayload, ByteString messageID, DatagramPacket packet)
            throws InterruptedException, IOException {
        boolean success = requestQueue.offer(
                new Request(requestPayload, messageID, packet),
                50,
                TimeUnit.MILLISECONDS
        );
        if (!success) {
            System.out.println("System overloaded");
            sendOverloadResponse(messageID, packet);
        }
    }

    private void sendResponse(byte[] msg, DatagramPacket packet) throws IOException {
        packet.setData(msg);
        socket.send(packet);
    }

    private void sendRouteRequest(
            ByteString messageID,
            KVRequest request,
            InetAddress address,
            int port
    ) throws IOException {
        long checkSum = Utils.getCheckSum(messageID.toByteArray(), request.toByteArray());
        Msg msg = Msg.newBuilder()
                .setMessageID(messageID)
                .setPayload(request.toByteString())
                .setCheckSum(checkSum)
                .build();
        byte[] msgByteArray = msg.toByteArray();
        DatagramPacket packet = new DatagramPacket(msgByteArray, msgByteArray.length, address, port);
        routeSocket.send(packet);
    }

    private void sendOverloadResponse(ByteString reqMessageID, DatagramPacket packet) throws IOException {
        KVResponse response = KVResponse.newBuilder()
                .setErrCode(ErrorCodes.TEMPORARY_SYSTEM_OVERLOAD)
                .setOverloadWaitTime(50)
                .build();
        long checkSum = Utils.getCheckSum(reqMessageID.toByteArray(), response.toByteArray());
        Msg msg = Msg.newBuilder()
                .setMessageID(reqMessageID)
                .setPayload(response.toByteString())
                .setCheckSum(checkSum)
                .build();
        sendResponse(msg.toByteArray(), packet);
    }
}
