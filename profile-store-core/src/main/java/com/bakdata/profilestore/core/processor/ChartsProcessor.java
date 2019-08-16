package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.core.ProfilestoreMain;
import com.bakdata.profilestore.core.avro.ChartTuple;
import com.bakdata.profilestore.core.avro.UserProfile;
import com.bakdata.profilestore.core.fields.FieldHandler;
import java.util.Comparator;
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

    public ChartsProcessor(final int chartSize, final FieldHandler fieldHandler) {
        this.chartSize = chartSize;
        this.fieldHandler = fieldHandler;
    }

    @Override
    public void init(final ProcessorContext processorContext) {
        this.profileStore =
                (KeyValueStore<Long, UserProfile>) processorContext
                        .getStateStore(ProfilestoreMain.PROFILE_STORE_NAME);
    }

    @Override
    public void process(final Long userId, final ChartTuple chartTuple) {
        final UserProfile profile = this.profileStore.get(userId);
        final List<ChartTuple> charts = this.fieldHandler.getCharts(profile);
        final UserProfile updatedProfile =
                this.fieldHandler.updateProfile(profile, this.updateCharts(charts, chartTuple));
        this.profileStore.put(userId, updatedProfile);
    }


    private List<ChartTuple> updateCharts(final List<ChartTuple> charts, final ChartTuple newCount) {
        boolean wasAdded = false;
        for (int i = 0; i < charts.size(); i++) {
            if (charts.get(i).getId() == newCount.getId()) {
                if (charts.get(i).getCountPlays() > newCount.getCountPlays()) {
                    return charts;
                } else {
                    charts.set(i, newCount);
                    wasAdded = true;
                    break;
                }
            }
        }
        if (charts.isEmpty() || !wasAdded) {
            charts.add(newCount);
        }
        charts.sort(Comparator.comparingLong(ChartTuple::getCountPlays).reversed());
        final int index = charts.size() <= this.chartSize ? charts.size() : charts.size() - 1;
        return charts.subList(0, index);
    }

    @Override
    public void close() {
    }
}