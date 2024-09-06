package io.github.fakeyanss.capacityawarequeueredis.core.consumer.dynamicadjuster;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * description 分配每个优先级的最大拉取记录值
 *
 * @author guichen01
 *         create date: 2020/8/13
 */
public class ExpMaxPollDistributor implements IMaxPollDistributor {

    private static final ExpMaxPollDistributor INSTANCE = new ExpMaxPollDistributor();

    private ExpMaxPollDistributor() {
    }

    public static ExpMaxPollDistributor instance() {
        return INSTANCE;
    }

    @Override
    public Map<Integer, Integer> distribution(int maxPriority, int maxPollRecords) {
        int sum = (1 << maxPriority) - 1;
        if (maxPollRecords < sum) {
            throw new IllegalArgumentException("Ensure maxPollRecords " + maxPollRecords + " is at least " + sum);
        }
        Map<Integer, Integer> maxPollRecordsDistribution = Maps.newHashMapWithExpectedSize(maxPriority);
        int left = maxPollRecords;
        for (int i = maxPriority - 1; i >= 0; --i) {
            int assign = (maxPollRecords * (1 << i)) / sum;
            maxPollRecordsDistribution.put(i, assign);
            left -= assign;
        }
        // Assign excess to priority {maxPriority - 1};
        maxPollRecordsDistribution.put(
                maxPriority - 1,
                maxPollRecordsDistribution.get(maxPriority - 1) + left);
        return maxPollRecordsDistribution;
    }

    public static void main(String[] args) {
        System.out.println(ExpMaxPollDistributor.instance().distribution(5, 100));
    }

}
