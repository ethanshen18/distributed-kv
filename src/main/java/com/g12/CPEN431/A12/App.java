package com.g12.CPEN431.A12;

import com.g12.CPEN431.A12.records.Address;
import com.g12.CPEN431.A12.records.KVStoreValue;
import com.g12.CPEN431.A12.records.Request;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.ByteString;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.g12.CPEN431.A12.utils.HashUtils.NUM_BUCKETS;

public class App {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("Usage: java -jar -Xmx512m A12.jar <ipAddress> <port>");
            return;
        }
        int port = Integer.parseInt(args[1]);
        DatagramSocket socket = new DatagramSocket(port);
        socket.setReceiveBufferSize(16_000);
        socket.setSendBufferSize(16_000);

        // Quorum socket, port offset by -3000
        DatagramSocket quorumSocket = new DatagramSocket(port - 3000);
        socket.setReceiveBufferSize(16_000);
        socket.setSendBufferSize(16_000);

        List<Address> servers = new ArrayList<>();
        BufferedReader serverReader = new BufferedReader(new FileReader("servers.txt"));
        String line = serverReader.readLine();
        while (line != null) {
            String[] address = line.split(":");
            int serverPort = Integer.parseInt(address[1]);
            servers.add(new Address(InetAddress.getByName(address[0]), serverPort, serverPort - 3000));
            line = serverReader.readLine();
        }
        serverReader.close();
        NUM_BUCKETS = servers.size();
        Address myAddress = new Address(InetAddress.getByName(args[0]), port, port - 3000);

        NodeStatus nodeStatus = new NodeStatus(servers, myAddress);
        BlockingQueue<Request> requestQueue = new ArrayBlockingQueue<>(100);
        Cache<ByteString, ByteString> cache = CacheBuilder.newBuilder()
                .maximumSize(70_000)
                .expireAfterWrite(1_000, TimeUnit.MILLISECONDS)
                .build();
        ConcurrentHashMap<ByteString, KVStoreValue> keyValuesMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, Set<ByteString>> replicaKeyMap = new ConcurrentHashMap<>();
        Runtime runtime = Runtime.getRuntime();

        for (int i = 0; i < 2; i++) {
            DatagramSocket routeSocket = new DatagramSocket();
            new Thread(new LoadBalancer(socket, routeSocket, nodeStatus, requestQueue, cache), "Load balancer " + i).start();
        }

        final int pid = (int) ProcessHandle.current().pid();
        for (int i = 0; i < 50; i++) {
            new Thread(new Server(cache, requestQueue, keyValuesMap, replicaKeyMap, nodeStatus, runtime, pid), "Server " + i).start();
        }

        if (NUM_BUCKETS > 1) {
            new Thread(new MembershipService(nodeStatus), "Membership Service").start();
            new Thread(new ReplicationService(nodeStatus, keyValuesMap, replicaKeyMap), "Replication Service").start();

            for (int i = 0; i < 6; i++) {
                DatagramSocket routeSocket = new DatagramSocket();
                new Thread(new LoadBalancer(quorumSocket, routeSocket, nodeStatus, requestQueue, cache), "Quorum Load balancer " + i).start();
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            socket.close();
        }));

        System.out.println("Server running on port " + socket.getLocalPort());
    }
}
