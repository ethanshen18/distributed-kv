package com.g12.CPEN431.A12.records;

import com.google.protobuf.ByteString;

public record KVStoreValue(ByteString value, int version, long timestamp) {
}
