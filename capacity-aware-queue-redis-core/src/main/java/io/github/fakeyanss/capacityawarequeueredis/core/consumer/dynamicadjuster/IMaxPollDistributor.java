package io.github.fakeyanss.capacityawarequeueredis.core.consumer.dynamicadjuster;

import java.util.Map;

public interface IMaxPollDistributor {

    Map<Integer, Integer> distribution(int maxPriority, int maxPollRecords);

}
