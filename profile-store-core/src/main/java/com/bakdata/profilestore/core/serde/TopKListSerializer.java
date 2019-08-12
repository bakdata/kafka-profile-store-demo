package com.bakdata.profilestore.core.serde;

import com.bakdata.profilestore.core.TopKList;
import com.bakdata.profilestore.core.avro.ChartTuple;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

public class TopKListSerializer implements Serializer<TopKList> {
    @Override
    public void configure(final Map<String, ?> map, final boolean bool) {
    }

    @Override
    public byte[] serialize(final String string, final TopKList topKList) {
        final int k = topKList.getK();
        final int size = topKList.size();
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
            this.writeTopKList(topKList, k, size, dataOutputStream);
        } catch (final IOException exception) {
            throw new RuntimeException("unable to serialize TopKiList", exception);
        }
        return byteArrayOutputStream.toByteArray();
    }

    private void writeTopKList(final TopKList topKList, final int k, final int size, final DataOutputStream dataOutputStream)
            throws IOException {
        final List<ChartTuple> values = topKList.values();
        dataOutputStream.writeInt(k);
        dataOutputStream.writeInt(size);
        for (final ChartTuple chartTuple : values) {
            final byte[] bytes = chartTuple.toByteBuffer().array();
            dataOutputStream.writeInt(bytes.length);
            dataOutputStream.write(bytes);
        }
    }

    @Override
    public void close() {
    }
}
