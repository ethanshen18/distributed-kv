package com.g12.CPEN431.A12.utils;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest.KVRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;
import ca.NetSysLab.ProtocolBuffers.Message.Msg;
import com.google.protobuf.ByteString;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.zip.CRC32;

public class Utils {
    private static final byte[] localAddress;       // Do not calculate this each time.

    static {
        try {
            localAddress = InetAddress.getLocalHost().getHostName().getBytes();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get checksum value from messageID and payload
     *
     * @param messageID unique random ID
     * @param payload   message payload in bytes
     * @return Checksum value
     * @throws IOException if an I/O error occurs
     */
    public static long getCheckSum(byte[] messageID, byte[] payload) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(messageID);
        output.write(payload);
        CRC32 cs = new CRC32();
        cs.update(output.toByteArray());
        return cs.getValue();
    }

    /**
     * Slice a segment of a byte array
     *
     * @param bytes byte array
     * @param off   offset
     * @param len   length to slice off
     * @return Sliced byte array
     */
    public static byte[] sliceBytes(byte[] bytes, int off, int len) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(bytes, off, len);
        return outputStream.toByteArray();
    }

    /**
     * Converts long to byte array.
     *
     * @param l long to convert
     * @return byte array representation of long
     */
    public static byte[] longToBytes(long l) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES);
        byteBuffer.putLong(l);
        return byteBuffer.array();
    }


    /**
     * Construct request unique ID with the following components:
     * <p>
     * <ul>
     *     <li>Client IP: 4 bytes</li>
     *     <li>Port: 2 bytes</li>
     *     <li>Random bytes: 2 bytes</li>
     *     <li>Time: 8 bytes</li>
     * </ul>
     *
     * @param socket DatagramSocket object
     * @return Unique ID byte array
     * @throws IOException if an issue occurs with local address or byte streams
     */
    public static byte[] constructUniqueID(DatagramSocket socket) throws IOException {

        byte[] portBytes = new byte[4];
        int localPort = socket.getLocalPort();
        ByteOrder.int2leb(localPort, portBytes, 0);

        byte[] randBytes = new byte[2];
        Random rd = new Random();
        rd.nextBytes(randBytes);

        long requestTime = System.nanoTime();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(localAddress, 0, 4);
        outputStream.write(portBytes, 0, 2);
        outputStream.write(randBytes, 0, 2);
        outputStream.write(longToBytes(requestTime));
        return outputStream.toByteArray();
    }

    /**
     * Construct a request message with a given socket and payload.
     *
     * @param socket - The socket the request will be sent from.
     * @param payload - The KVRequest payload object.
     * @return - The request Msg.
     * @throws IOException - This method may throw IOException.
     */
    public static Msg constructRequestMsg(DatagramSocket socket, KVRequest payload) throws IOException {
        byte[] messageID = constructUniqueID(socket);

        long checksum = getCheckSum(messageID, payload.toByteArray());

        return Msg.newBuilder()
                .setMessageID(ByteString.copyFrom(messageID))
                .setPayload(payload.toByteString())
                .setCheckSum(checksum)
                .build();
    }

    /**
     * Sends an inter-server request to a target node specified by destAddress and destPort.
     *
     * @param socket - The sender's socket.
     * @param req - The request Msg.
     * @param destAddress - The target node address.
     * @param destPort - The target node port.
     * @return - The KCResponse payload object.
     * @throws IOException - This method may throw IOException.
     */
    public static KVResponse sendRequest(
            DatagramSocket socket,
            Msg req,
            InetAddress destAddress,
            int destPort
    ) throws IOException {
        byte[] reqBytes = req.toByteArray();
        DatagramPacket reqPacket = new DatagramPacket(
                reqBytes,
                reqBytes.length,
                destAddress,
                destPort
        );

        DatagramPacket resPacket = new DatagramPacket(new byte[16_000], 16_000);

        int timeout = 100;

        for (int i = 0; i < 4; i++) {
            socket.setSoTimeout(timeout);
            socket.send(reqPacket);
            boolean flag;
            try {
                do {
                    flag = false;
                    socket.receive(resPacket);
                    Msg res = Msg.parseFrom(Arrays.copyOfRange(resPacket.getData(), 0, resPacket.getLength()));

                    // build checksum
                    long expChecksum = Utils.getCheckSum(res.getMessageID().toByteArray(), res.getPayload().toByteArray());

                    // check response
                    if (req.getMessageID().equals(res.getMessageID()) && expChecksum == res.getCheckSum()) {
                        return KVResponse.parseFrom(res.getPayload());
                    } else if (!req.getMessageID().equals(res.getMessageID())) {
                        flag = true;
                    }
                } while (flag);
            } catch (SocketTimeoutException e) {
                timeout *= 2;
            }
        }

        return null;
    }

    public static void sendRequestNoWait(
            DatagramSocket socket,
            Msg msg,
            InetAddress address,
            int port
    ) throws IOException {
        byte[] msgByteArray = msg.toByteArray();
        DatagramPacket packet = new DatagramPacket(msgByteArray, msgByteArray.length, address, port);
        socket.send(packet);
    }
}
