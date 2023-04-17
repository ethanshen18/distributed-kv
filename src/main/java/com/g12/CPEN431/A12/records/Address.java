package com.g12.CPEN431.A12.records;

import java.net.InetAddress;

public record Address(InetAddress address, int port, int quorumPort) {
}
