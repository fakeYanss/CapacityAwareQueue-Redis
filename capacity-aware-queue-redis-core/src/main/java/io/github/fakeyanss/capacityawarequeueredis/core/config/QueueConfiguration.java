package io.github.fakeyanss.capacityawarequeueredis.core.config;

import io.github.fakeyanss.capacityawarequeueredis.core.consumer.MsgConsumer;
import lombok.Builder;
import lombok.Getter;

/**
 * description
 *
 * @author guichen01
 *         create date: 2020/8/11
 */
@Builder
@Getter
public class QueueConfiguration {

    /**
     * 队列名称
     */
    private String queue;
    /**
     * 消费者
     */
    private MsgConsumer consumer;

    private int priority;

}