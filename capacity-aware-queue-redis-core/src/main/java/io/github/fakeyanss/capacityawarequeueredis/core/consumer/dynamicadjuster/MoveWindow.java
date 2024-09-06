package io.github.fakeyanss.capacityawarequeueredis.core.consumer.dynamicadjuster;

import java.util.Arrays;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * description 滑动窗口，记录历史拉取数量，计算可调整的拉取记录最大值
 *
 * @author guichen01
 *         create date: 2020/8/12
 */
public class MoveWindow {

    /**
     * Current index;
     */
    private int circularIdx;
    /**
     * Values in the window;
     */
    private int[] values;
    /**
     * Captures sorted value along with freq;
     */
    private SortedMap<Integer, Integer> sortedValues;
    /**
     * Threshold count of how many values are maxed out;
     */
    private int maxedOutCountThreshold;
    /**
     * Max limit value or maxed out value;
     */
    private int valueLimit;
    /**
     * Number of values that are maxed out;
     */
    private int maxedOutCount;

    public MoveWindow(final int size, int maxedOutCountThreshold, int valueLimit) {
        this.circularIdx = -1;
        this.values = new int[size];
        this.sortedValues = new TreeMap<Integer, Integer>() {
            {
                put(0, size);
            }
        };
        this.maxedOutCountThreshold = maxedOutCountThreshold;
        this.valueLimit = valueLimit;
        this.maxedOutCount = 0;
    }

    /**
     * Add a value to the circular buffer;
     */
    public void add(int value) {
        int nextIdx = (circularIdx + 1) % values.length;

        // Update maxedOutCount;
        if (value >= valueLimit && values[nextIdx] < valueLimit) {
            ++maxedOutCount;
        } else if (value < valueLimit && values[nextIdx] >= valueLimit) {
            --maxedOutCount;
        }

        // Update sortedValues;
        sortedValues.put(values[nextIdx], sortedValues.get(values[nextIdx]) - 1);
        if (sortedValues.get(values[nextIdx]) == 0) {
            sortedValues.remove(values[nextIdx]);
        }
        if (!sortedValues.containsKey(value)) {
            sortedValues.put(value, 0);
        }
        sortedValues.put(value, sortedValues.get(value) + 1);

        values[nextIdx] = value;
        circularIdx = nextIdx;
    }

    /**
     * Check if {@code maxedOutCount} &ge; {@code maxedOutCountThreshold}.
     */
    public boolean isMaxedOutThresholdBreach() {
        return maxedOutCount >= maxedOutCountThreshold;
    }

    /**
     * Return max unused value so far w.r.t. {@code valueLimit}.
     */
    public int maxUnusedValue() {
        return Math.max(0, valueLimit - sortedValues.lastKey());
    }

    @Override
    public String toString() {
        return "-----------------------------------" + "\n" +
                "circularIdx: " + this.circularIdx + "\n" +
                "values: " + Arrays.toString(this.values) + "\n" +
                "sortedValues: " + this.sortedValues + "\n" +
                "maxedOutCountThreshold: " + this.maxedOutCountThreshold + "\n" +
                "valueLimit: " + this.valueLimit + "\n" +
                "maxedOutCount: " + this.maxedOutCount + "\n" +
                "isMaxedOutThresholdBreach: " + this.isMaxedOutThresholdBreach() + "\n" +
                "maxUnusedValue: " + this.maxUnusedValue() + "\n" +
                "-----------------------------------";
    }

    public static void main(String[] args) {
        MoveWindow moveWindow = new MoveWindow(6, 4, 7);
        Random random = new Random();
        for (int i = 1; i <= 100; ++i) {
            moveWindow.add(Math.abs(random.nextInt() % 10));
            System.out.println(moveWindow.toString());
        }
    }

}
