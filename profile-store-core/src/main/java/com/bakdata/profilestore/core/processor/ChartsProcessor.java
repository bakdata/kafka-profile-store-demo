package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.core.FieldType;
import com.bakdata.profilestore.core.ProfilestoreMain;
import com.bakdata.profilestore.core.avro.ChartTuple;
import com.bakdata.profilestore.core.avro.UserProfile;
import com.bakdata.profilestore.core.fields.FieldHandler;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class ChartsProcessor implements Processor<Long, ChartTuple> {
    private KeyValueStore<Long, UserProfile> profileStore;
    private final int chartSize;
    private final FieldHandler fieldHandler;
    private ProcessorContext context;

    public ChartsProcessor(final int chartSize, final FieldHandler fieldHandler) {
        this.chartSize = chartSize;
        this.fieldHandler = fieldHandler;
    }

    @Override
    public void init(final ProcessorContext processorContext) {
        this.profileStore =
                (KeyValueStore<Long, UserProfile>) processorContext
                        .getStateStore(ProfilestoreMain.PROFILE_STORE_NAME);
        this.context = processorContext;
    }

    @Override
    public void process(final Long userId, final ChartTuple chartTuple) {
        log.info("partition {}: process user {}", this.context.partition(), userId);
        final UserProfile profile = this.profileStore.get(userId);
        final List<ChartTuple> charts = this.fieldHandler.getCharts(profile);
        final UserProfile updatedProfile =
                this.fieldHandler.updateProfile(profile, this.updateCharts(charts, chartTuple));
        this.profileStore.put(userId, updatedProfile);
    }


    private List<ChartTuple> updateCharts(final List<ChartTuple> charts, final ChartTuple newCount) {
        final ChartTuple oldCount = new ChartTuple(newCount.getId(), newCount.getCountPlays() - 1);

        if (charts.contains(oldCount)) {
            charts.set(charts.indexOf(oldCount), newCount);
            return charts;
        } else {
            charts.add(newCount);
            Collections.sort(charts);
            final int index = charts.size() < this.chartSize ? charts.size() : charts.size() - 1;
            return charts.subList(0, index);
        }

    }

    @Override
    public void close() {
    }
}
