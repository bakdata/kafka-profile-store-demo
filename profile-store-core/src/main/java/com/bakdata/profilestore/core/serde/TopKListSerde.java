package com.bakdata.profilestore.core.serde;

import com.bakdata.profilestore.core.TopKList;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class TopKListSerde implements Serde<TopKList> {
    private final Serde<TopKList> inner;

    public TopKListSerde() {
        this.inner = Serdes.serdeFrom(new TopKListSerializer(), new TopKListDeserializer());
    }

    @Override
    public void configure(final Map<String, ?> map, final boolean b) {
        this.inner.serializer().configure(map, b);
        this.inner.deserializer().configure(map, b);
    }

    @Override
    public void close() {
        this.inner.serializer().close();
        this.deserializer().close();
    }

    @Override
    public Serializer<TopKList> serializer() {
        return this.inner.serializer();
    }

    @Override
    public Deserializer<TopKList> deserializer() {
        return this.inner.deserializer();
    }

}
