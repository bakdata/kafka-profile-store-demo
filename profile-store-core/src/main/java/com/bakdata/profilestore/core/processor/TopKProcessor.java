package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.common.FieldType;
import com.bakdata.profilestore.common.avro.ListeningEvent;
import com.bakdata.profilestore.core.ProfilestoreTopology;
import com.bakdata.profilestore.core.TopKList;
import com.bakdata.profilestore.core.avro.ChartTuple;
import com.bakdata.profilestore.core.avro.CompositeKey;
import com.bakdata.profilestore.core.avro.UserProfile;
import com.bakdata.profilestore.core.fields.FieldHandler;
import java.util.List;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class TopKProcessor implements Processor<byte[], ListeningEvent> {
    private KeyValueStore<CompositeKey, Long> countStore;
    private KeyValueStore<Long, TopKList> chartStore;
    private KeyValueStore<Long, UserProfile> profileStore;
    private final int k;
    private final FieldHandler fieldHandler;

    public TopKProcessor(final int k, final FieldHandler fieldHandler) {
        this.k = k;
        this.fieldHandler = fieldHandler;
    }

    @Override
    public void init(final ProcessorContext processorContext) {
        this.countStore =
                (KeyValueStore<CompositeKey, Long>) processorContext
                        .getStateStore(getCountStoreName(this.fieldHandler.type()));
        this.chartStore =
                (KeyValueStore<Long, TopKList>) processorContext
                        .getStateStore(getChartStoreName(this.fieldHandler.type()));
        this.profileStore =
                (KeyValueStore<Long, UserProfile>) processorContext
                        .getStateStore(ProfilestoreTopology.PROFILE_STORE_NAME);

    }

    @Override
    public void process(final byte[] key, final ListeningEvent event) {
        final long fieldId = this.fieldHandler.extractId(event);
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
        this.updateCharts(event, fieldId, count, topKList);
        this.updateProfile(event, topKList);

    }

    private void updateCharts(final ListeningEvent event, final long fieldId, final Long count, final TopKList topKList) {
        topKList.remove(new ChartTuple(fieldId, count));
        topKList.add(new ChartTuple(fieldId, count + 1));
        this.chartStore.put(event.getUserId(), topKList);
    }

    private void updateProfile(final ListeningEvent event, final TopKList topKList) {
        final List<ChartTuple> charts = topKList.values();
        final UserProfile userProfile = this.profileStore.get(event.getUserId());
        final UserProfile updatedProfile = this.fieldHandler.updateProfile(userProfile, charts);
        this.profileStore.put(event.getUserId(), updatedProfile);
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
