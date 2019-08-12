package com.bakdata.profilestore.core;

import com.bakdata.profilestore.core.avro.ChartTuple;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import lombok.Getter;

public class TopKList {
    private final PriorityQueue<ChartTuple> priorityQueue;
    @Getter
    private final int k;

    public TopKList(final int k) {
        this.priorityQueue = new PriorityQueue<>(k, Comparator.comparingLong(ChartTuple::getCountPlays));
        this.k = k;
    }

    public void add(final ChartTuple newTuple) {
        if (this.priorityQueue.size() < this.k) {
            this.priorityQueue.add(newTuple);
        } else {
            final ChartTuple currentLast = this.priorityQueue.peek();
            if (currentLast != null && newTuple.getCountPlays() >= currentLast.getCountPlays()) {
                this.priorityQueue.remove(currentLast);
                this.priorityQueue.add(newTuple);
            }
        }
    }

    public void remove(final ChartTuple oldTuple) {
        this.priorityQueue.remove(oldTuple);
    }

    public List<ChartTuple> values() {
        final List<ChartTuple> list = new ArrayList<>();
        while (!this.priorityQueue.isEmpty()) {
            list.add(this.priorityQueue.poll());
        }
        Collections.reverse(list);
        return list;
    }

    public int size() {
        return this.priorityQueue.size();
    }

    @Override
    public String toString() {
       return this.priorityQueue.toString();
    }
}
