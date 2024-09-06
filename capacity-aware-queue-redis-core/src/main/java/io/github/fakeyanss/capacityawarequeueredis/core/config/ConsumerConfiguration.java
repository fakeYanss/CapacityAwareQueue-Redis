package io.github.fakeyanss.capacityawarequeueredis.core.config;

import lombok.Data;

/**
 * description
 *
 * @author guichen01
 *         create date: 2020/8/13
 */
@Data
public class ConsumerConfiguration {

    // pool超时时间
    private int pollTimeout;
    // 每批次最大拉取记录数
    private int maxPollRecords;
    // 最大移动窗口历史记录次数
    private int maxPollHistoryWindowSize;
    // 最小移动窗口调整阈值
    private int minPollWindowMaxOutThreshold;
    private String customConsumerName;

}
