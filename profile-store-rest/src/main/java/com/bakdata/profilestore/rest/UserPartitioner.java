package com.bakdata.profilestore.rest;

import java.nio.ByteBuffer;
import org.apache.kafka.common.utils.Utils;

public class UserPartitioner {
    public int partition(final long userId, final int numPartitions) {
        final byte[] keyBytes = keyToByteArray(userId);
        // see org.apache.kafka.clients.producer.internals.DefaultPartitioner
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }

    private static byte[] keyToByteArray(final long key) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(key);
        return buffer.array();
    }
}
