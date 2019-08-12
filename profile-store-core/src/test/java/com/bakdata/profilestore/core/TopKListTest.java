package com.bakdata.profilestore.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.bakdata.profilestore.core.avro.ChartTuple;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.hamcrest.MatcherAssert;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.jupiter.api.Test;

class TopKListTest {

    @Test
    void testLimit() {
        final Random random = new Random();
        final TopKList topKList = new TopKList(10);
        Stream.generate(() -> new ChartTuple(random.nextLong(), random.nextLong())).limit(20).forEach(topKList::add);
        assertEquals(10, topKList.size());
    }

    @Test
    void testOrdering() {
        final long[] values = {20L, 30L, 25L, 7L, 100L, 50L};
        final TopKList topKList = new TopKList(3);
        final List<ChartTuple> input = IntStream.range(0, values.length)
                .mapToObj(i -> new ChartTuple((long) i, values[i]))
                .collect(Collectors.toList());
        input.forEach(topKList::add);
        final ChartTuple[] tuples = input.toArray(ChartTuple[]::new);

        MatcherAssert.assertThat(topKList.values(),
                IsIterableContainingInOrder.contains(tuples[4], tuples[5], tuples[1]));


    }

}