package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.core.ProfilestoreMain;
import com.bakdata.profilestore.core.avro.ChartRecord;
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
public class ChartsProcessor implements Processor<Long, ChartRecord> {
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
    public void process(final Long userId, final ChartRecord chartRecord) {
        final UserProfile profile = this.profileStore.get(userId);
        final List<ChartRecord> charts = this.fieldHandler.getCharts(profile);
        final UserProfile updatedProfile =
                this.fieldHandler.updateProfile(profile, this.updateCharts(charts, chartRecord));
        this.profileStore.put(userId, updatedProfile);
    }

    private List<ChartRecord> updateCharts(final List<ChartRecord> charts, final ChartRecord newRecord) {
        if (charts.isEmpty()) {
            return Collections.singletonList(newRecord);
        }
        // if the charts are full and the last element has more plays than the new record
        // there is no change
        else if (charts.size() == this.chartSize
                && charts.get(this.chartSize - 1).getCountPlays() > newRecord.getCountPlays()) {
            return charts;
        } else {
            return this.addNewCount(charts, newRecord);
        }
    }

    private List<ChartRecord> addNewCount(final List<ChartRecord> charts, final ChartRecord newRecord) {
        boolean wasAdded = false;
        final List<ChartRecord> updatedCharts = new ArrayList<>(this.chartSize);
        for (final ChartRecord currentTuple : charts) {
            if (!wasAdded && newRecord.getCountPlays() >= currentTuple.getCountPlays()) {
                updatedCharts.add(newRecord);
                wasAdded = true;
            }

            if (updatedCharts.size() < this.chartSize) {
                // add current record if it has more plays or
                // it has a different id and the new record was already added
                if (currentTuple.getCountPlays() > newRecord.getCountPlays() ||
                        (wasAdded && currentTuple.getId() != newRecord.getId())) {
                    updatedCharts.add(currentTuple);
                }
            }
        }

        // add if new the record has the smallest count but charts are not full yet
        if (!wasAdded && updatedCharts.size() < this.chartSize) {
            updatedCharts.add(newRecord);
        }

        return updatedCharts;
    }

    @Override
    public void close() {
    }
}
