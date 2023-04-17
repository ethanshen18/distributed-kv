package com.g12.CPEN431.A12;

import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;


/**
 * A pool of datagram sockets that can be used to send and receive messages.
 */
public class SocketPool {
    private final Semaphore semaphore;
    private final ConcurrentLinkedQueue<DatagramSocket> sockets = new ConcurrentLinkedQueue<>();
    private final int maxSockets;

    /**
     * Creates a new socket pool with the specified maximum number of sockets.
     * @param maxSockets the maximum number of sockets that can be used at the same time
     */
    public SocketPool(int maxSockets) {
        semaphore = new Semaphore(maxSockets);
        this.maxSockets = maxSockets;
    }

    /**
     * Gets a socket from the pool. If there are no sockets available, a new one will be created.
     * If there are no more permits, an InsufficientResourcesException will be thrown.
     *
     * @return socket from socketpool
     * @throws InsufficientResourcesException
     */
    public DatagramSocket getSocket() throws InsufficientResourcesException {
        if (!semaphore.tryAcquire()){
            throw new InsufficientResourcesException("No more sockets available");
        }
        DatagramSocket socket = sockets.poll();
        if (socket != null) {
            return socket;
        }
        try {
            return new DatagramSocket();
        } catch (SocketException e) {
            semaphore.release();
            throw new RuntimeException("Failed to create socket", e);
        }
    }

    /**
     * Returns a socket to the pool. If there are more than 50% more sockets in the pool than being used,
     * the socket will be closed.
     * @param socket socket to return to pool
     */
    public void returnSocket(DatagramSocket socket) {
        // if there are >50% more sockets in the pool than being used, close the socket
        if (sockets.size() > (maxSockets - semaphore.availablePermits()) + (maxSockets - semaphore.availablePermits()) / 2) {
            socket.close();
        } else {
            sockets.add(socket);
            semaphore.release();
        }
    }

}
