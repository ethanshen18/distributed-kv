package com.g12.CPEN431.A12;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest.KVRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;
import ca.NetSysLab.ProtocolBuffers.Message.Msg;
import com.g12.CPEN431.A12.codes.Command;
import com.g12.CPEN431.A12.records.Address;
import com.g12.CPEN431.A12.utils.Utils;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import static com.g12.CPEN431.A12.utils.HashUtils.NUM_BUCKETS;

public class MembershipService implements Runnable {
    private static final int NODE_PINGS_PER_CYCLE = 5;
    private static final int WAIT_TIME_MILLIS = 400;
    private static final int STARTUP_DELAY_MILLIS = 2_000;
    private final DatagramSocket membershipServiceSocket;
    private final NodeStatus nodeStatus;

    private final Random rand;

    public MembershipService(NodeStatus nodeStatus) throws SocketException {
        this.membershipServiceSocket = new DatagramSocket();
        this.nodeStatus = nodeStatus;
        this.rand = new Random();
    }

    @Override
    public void run() {
        HashSet<Integer> targetNodes = new HashSet<>();

        // start-up delay
        try {
            Thread.sleep(STARTUP_DELAY_MILLIS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // push-pull epidemic protocol performed periodically
        while (true) try {

            // select set of nodes to ping
            while (targetNodes.size() < NODE_PINGS_PER_CYCLE) {

                // pick random index
                int index = rand.nextInt(NUM_BUCKETS);

                // skip if selected self
                if (index == nodeStatus.getMe())
                    continue;

                // insert into set
                targetNodes.add(index);
            }

            // send request to all target nodes in set
            for (int index : targetNodes) {

                // send request to target node
                Msg req = generateGetStatusRequest();
                Address targetAddress = nodeStatus.getAddress(index);
                KVResponse payload = Utils.sendRequest(
                        membershipServiceSocket,
                        req,
                        targetAddress.address(),
                        targetAddress.port()
                );

                // process received last-alive timestamps
                if (payload != null) {
                    // parse received timestamps
                    List<Long> newTimestamps = payload.getLastAliveTimestampsList();

                    // update local timestamps
                    if (newTimestamps.size() == NUM_BUCKETS) {
                        nodeStatus.updateLastAliveTimestamps(newTimestamps);
                    } else {
                        System.err.println("MembershipService received timestamps array length mismatch: " + newTimestamps.size());
                    }
                }
            }

            targetNodes.clear();

            // interval delay
            try {
                Thread.sleep(WAIT_TIME_MILLIS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Helper method to generate get status requests for the epidemic protocol.
     *
     * @return - The request Msg.
     * @throws IOException - This method may throw IOException.
     */
    private Msg generateGetStatusRequest() throws IOException {

        // push updated timestamps to other nodes
        KVRequest payload = KVRequest.newBuilder()
                .setCommand(Command.GET_STATUS)
                .addAllLastAliveTimestamps(nodeStatus.getLastAliveTimestamps())
                .build();

        return Utils.constructRequestMsg(membershipServiceSocket, payload);
    }

}
