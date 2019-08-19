package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.core.ProfilestoreMain;
import com.bakdata.profilestore.core.avro.ChartTuple;
import com.bakdata.profilestore.core.avro.UserProfile;
import com.bakdata.profilestore.core.fields.FieldHandler;
import java.util.ArrayList;
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
        if (charts.isEmpty()) {
            return Collections.singletonList(newCount);
        }
        // if the charts are full and the last element has more plays than the new tuple
        // there is no change
        else if (charts.size() == this.chartSize
                && charts.get(this.chartSize - 1).getCountPlays() > newCount.getCountPlays()) {
            return charts;
        } else {
            return this.addNewCount(charts, newCount);
        }
    }

    private List<ChartTuple> addNewCount(final List<ChartTuple> charts, final ChartTuple newCount) {
        boolean wasAdded = false;
        final List<ChartTuple> updatedCharts = new ArrayList<>(this.chartSize);
        for (final ChartTuple currentTuple : charts) {
            if (!wasAdded && newCount.getCountPlays() >= currentTuple.getCountPlays()) {
                updatedCharts.add(newCount);
                wasAdded = true;
            }

            if (updatedCharts.size() < this.chartSize) {
                // add current tuple if it has more plays or
                // it has a different id and the new tuple was already added
                if (currentTuple.getCountPlays() > newCount.getCountPlays() ||
                        (wasAdded && currentTuple.getId() != newCount.getId())) {
                    updatedCharts.add(currentTuple);
                }
            }
        }

        // add if new the tuple has the smallest count but charts are not full yet
        if (!wasAdded && updatedCharts.size() < this.chartSize) {
            updatedCharts.add(newCount);
        }

        return updatedCharts;
    }

    @Override
    public void close() {
    }
}
