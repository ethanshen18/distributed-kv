package com.g12.CPEN431.A12.records;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import com.google.protobuf.ByteString;

import java.net.DatagramPacket;

public record Request(KeyValueRequest.KVRequest payload, ByteString messageID, DatagramPacket packet) {
}
