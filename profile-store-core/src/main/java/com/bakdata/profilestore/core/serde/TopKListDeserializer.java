package com.bakdata.profilestore.core.serde;

import com.bakdata.profilestore.core.TopKList;
import com.bakdata.profilestore.core.avro.ChartTuple;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;

public class TopKListDeserializer implements Deserializer<TopKList> {
    @Override
    public void configure(final Map<String, ?> map, final boolean bool) {
    }

    @Override
    public TopKList deserialize(final String string, final byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        final TopKList topKList;
        try (final DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(bytes))) {
            topKList = this.readTopKList(inputStream);
        } catch (final IOException exception) {
            throw new RuntimeException("Unable to deserialize TopKList", exception);
        }
        return topKList;
    }

    private TopKList readTopKList(final DataInputStream inputStream) throws IOException {
        final TopKList topKList;
        final int k = inputStream.readInt();
        final int records = inputStream.readInt();
        topKList = new TopKList(k);
        for (int i = 0; i < records; i++) {
            final byte[] valueBytes = new byte[inputStream.readInt()];
            inputStream.read(valueBytes);
            final ChartTuple chartTuple = ChartTuple.fromByteBuffer(ByteBuffer.wrap(valueBytes));
            topKList.add(chartTuple);
        }
        return topKList;
    }


    @Override
    public void close() {
    }
}
