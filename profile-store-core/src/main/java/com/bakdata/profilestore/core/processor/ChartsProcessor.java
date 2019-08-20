package com.bakdata.profilestore.core.processor;

import com.bakdata.profilestore.core.ProfilestoreMain;
import com.bakdata.profilestore.core.avro.ChartRecord;
import com.bakdata.profilestore.core.avro.NamedChartRecord;
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
import org.jooq.lambda.Seq;

@Slf4j
public class ChartsProcessor implements Processor<Long, NamedChartRecord> {
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
    public void process(final Long userId, final NamedChartRecord chartRecord) {
        final UserProfile profile = DefaultUserProfile.getOrDefault(this.profileStore.get(userId));
        final List<NamedChartRecord> charts = this.fieldHandler.getCharts(profile);
        final UserProfile updatedProfile =
                this.fieldHandler.updateProfile(profile, this.updateCharts(charts, chartRecord));
        this.profileStore.put(userId, updatedProfile);
    }

    private List<NamedChartRecord> updateCharts(final List<NamedChartRecord> charts, final NamedChartRecord newRecord) {
        return Seq.concat(charts.stream(), Stream.of(newRecord))
                // if there are two records with the same id, remove the one with the smaller count
                .grouped(NamedChartRecord::getId)
                .map(tuple -> tuple.v2().max(Comparator.comparingLong(NamedChartRecord::getCountPlays)).get())
                .sorted(Comparator.comparingLong(NamedChartRecord::getCountPlays).reversed())
                .limit(this.chartSize)
                .collect(Collectors.toList());
    }

    @Override
    public void close() {
    }
}
