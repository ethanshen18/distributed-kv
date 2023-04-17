package com.g12.CPEN431.A12.utils;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashUtils {
    private static final String ALGORITHM = "MD5";
    public static int NUM_BUCKETS;

    private static byte[] hashKey(byte[] key) {
        try {
            MessageDigest digest = MessageDigest.getInstance(ALGORITHM);
            digest.update(key);
            return digest.digest();
        } catch (NoSuchAlgorithmException e) {
            // Will not happen
            throw new RuntimeException(e);
        }
    }

    private static int consistentHash(byte[] hashedKey) {
        HashCode hashCode = HashCode.fromBytes(hashedKey);
        return Hashing.consistentHash(hashCode, NUM_BUCKETS);
    }

    public static int getBucketFromKey(byte[] key) {
        return consistentHash(hashKey(key));
    }
}
