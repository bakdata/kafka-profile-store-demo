package com.bakdata.profilestore.recommender.algorithm;

import static org.hamcrest.MatcherAssert.assertThat;

import com.bakdata.profilestore.recommender.MockGraph;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SalsaTest {
    @Test
    void testCorrectLimit() {
        final Map<Long, List<Long>> leftIndex = new HashMap<>();
        leftIndex.put(1L, Arrays.asList(6L, 9L, 12L, 15L, 18L));
        leftIndex.put(2L, Arrays.asList(6L, 10L, 14L, 18L, 20L));
        leftIndex.put(3L, Arrays.asList(6L, 7L, 8L, 9L, 10L));
        leftIndex.put(4L, Arrays.asList(8L, 12L, 16L, 20L, 8L));
        leftIndex.put(5L, Arrays.asList(10L, 12L, 14L, 9L, 10L));

        final MockGraph mockGraph = MockGraph.fromLeftIndex(leftIndex);
        final List<Long> recommendations = new Salsa(mockGraph, new Random())
                .compute(3L, 100, 5, 0.1, 3);
        Assertions.assertEquals(3, recommendations.size());
    }

    @Test
    void testRecommendations() {
        final Map<Long, List<Long>> leftIndex = new HashMap<>();
        // every connection leads to 25 and 30
        leftIndex.put(1L, Arrays.asList(6L, 9L, 12L, 15L, 18L));
        leftIndex.put(2L, Arrays.asList(6L, 30L, 25L, 12L, 15L));
        leftIndex.put(3L, Arrays.asList(20L, 6L, 22L, 25L, 30L));
        leftIndex.put(4L, Arrays.asList(20L, 25L, 12L, 24L, 30L));
        leftIndex.put(5L, Arrays.asList(18L, 30L, 25L, 36L, 26L));

        final MockGraph mockGraph = MockGraph.fromLeftIndex(leftIndex);
        final List<Long> recommendations = new Salsa(mockGraph, new Random())
                .compute(1L, 100, 50, 0.1, 2);
        assertThat(recommendations, IsIterableContainingInAnyOrder.containsInAnyOrder(25L, 30L));
    }

    @Test
    void testFilter() {
        final Map<Long, List<Long>> leftIndex = new HashMap<>();
        // interacted with everything except one
        leftIndex.put(1L, Arrays.asList(6L, 9L, 12L, 15L, 18L));
        leftIndex.put(2L, Arrays.asList(6L, 9L, 12L, 15L, 18L));
        leftIndex.put(3L, Arrays.asList(6L, 9L, 12L, 15L, 18L));
        leftIndex.put(4L, Arrays.asList(6L, 9L, 12L, 15L, 18L));
        leftIndex.put(5L, Arrays.asList(6L, 9L, 12L, 15L, 19L));

        final MockGraph mockGraph = MockGraph.fromLeftIndex(leftIndex);
        final List<Long> recommendations = new Salsa(mockGraph, new Random())
                .compute(1L, 100, 50, 0.1, 10);
        Assertions.assertEquals(Collections.singletonList(19L), recommendations);
    }
}