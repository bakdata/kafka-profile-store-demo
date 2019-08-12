package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.common.FieldType;
import com.bakdata.profilestore.core.TopKList;
import com.bakdata.profilestore.core.avro.ChartTuple;
import com.bakdata.profilestore.core.avro.CompositeKey;
import com.bakdata.profilestore.core.ids.IdExtractor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class TopKProcessor implements Processor<byte[], ListeningEvent> {
    private KeyValueStore<CompositeKey, Long> countStore;
    private KeyValueStore<Long, TopKList> chartStore;
    private final int k;
    private final IdExtractor idExtractor;

    public TopKProcessor(final int k, final IdExtractor idExtractor) {
        this.k = k;
        this.idExtractor = idExtractor;
    }

    @Override
    public void init(final ProcessorContext processorContext) {
        this.countStore =
                (KeyValueStore<CompositeKey, Long>) processorContext
                        .getStateStore(getCountStoreName(this.idExtractor.type()));
        this.chartStore =
                (KeyValueStore<Long, TopKList>) processorContext.getStateStore(getChartStoreName(this.idExtractor.type()));

    }

    @Override
    public void process(final byte[] key, final ListeningEvent event) {
        final long fieldId = this.idExtractor.extractId(event);
        final CompositeKey compositeKey = new CompositeKey(event.getUserId(), fieldId);

        Long count = this.countStore.get(compositeKey);
        if (count == null) {
            count = 0L;
        }
        this.countStore.put(compositeKey, count + 1);

        TopKList topKList = this.chartStore.get(event.getUserId());
        if (topKList == null) {
            topKList = new TopKList(this.k);
        }
        topKList.remove(new ChartTuple(fieldId, count));
        topKList.add(new ChartTuple(fieldId, count + 1));
        this.chartStore.put(event.getUserId(), topKList);

    }

    @Override
    public void close() {
    }

    public static String getCountStoreName(final FieldType type) {
        return "count_store_" + type;
    }

    public static String getChartStoreName(final FieldType type) {
        return "chart_store_" + type;
    }

    public static String getProcessorName(final FieldType type) {
        return "topK_processor_" + type;
    }
}
