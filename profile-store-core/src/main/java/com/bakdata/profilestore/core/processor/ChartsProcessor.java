package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.core.ProfilestoreMain;
import com.bakdata.profilestore.core.avro.ChartRecord;
import com.bakdata.profilestore.core.avro.UserProfile;
import com.bakdata.profilestore.core.fields.FieldHandler;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
        final UserProfile profile = DefaultUserProfile.getOrDefault(this.profileStore.get(userId));
        final List<ChartRecord> charts = this.fieldHandler.getCharts(profile);
        final UserProfile updatedProfile =
                this.fieldHandler.updateProfile(profile, this.updateCharts(charts, chartRecord));
        this.profileStore.put(userId, updatedProfile);
    }

    private List<ChartRecord> updateCharts(final List<ChartRecord> charts, final ChartRecord newRecord) {
        return Stream
                .concat(charts.stream(), Stream.of(newRecord))
                .sorted(Comparator.comparing(ChartRecord::getCountPlays))
                .limit(this.chartSize)
                .collect(Collectors.toList());
    }

    @Override
    public void close() {
    }
}
