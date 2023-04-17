package com.g12.CPEN431.A12;

import com.g12.CPEN431.A12.records.Request;

import java.util.concurrent.BlockingQueue;

public class Monitor implements Runnable{
    BlockingQueue<Request> requestQueue;
    public Monitor(BlockingQueue<Request> requestQueue) {
        this.requestQueue = requestQueue;
    }

    @Override
    public void run() {
        while (true) {
            float requestQueueUsage = (float)requestQueue.size() / (float)(requestQueue.size() + requestQueue.remainingCapacity());
            System.out.println("Request Queue Usage: " + requestQueueUsage);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
