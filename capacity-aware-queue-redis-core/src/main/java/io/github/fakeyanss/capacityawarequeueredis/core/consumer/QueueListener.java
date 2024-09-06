package io.github.fakeyanss.capacityawarequeueredis.core.consumer;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.core.RedisTemplate;

import io.github.fakeyanss.capacityawarequeueredis.core.consumer.dynamicadjuster.MoveWindow;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * description 队列监听器
 * 消费指定redis list的数据
 *
 * @author guichen01
 *         create date: 2020/8/11
 */
@Slf4j
@Getter
@Builder
public class QueueListener implements Runnable {

    private RedisTemplate<Object, Object> redisTemplate;
    private String queue;
    private MsgConsumer consumer;
    private int priority;
    private AtomicInteger quota;
    private AtomicInteger recordCount;
    private MoveWindow moveWindow;
    private int pollTimeout;

    public int getRecordCount() {
        return recordCount.get();
    }

    public void resetQuota(int newQuota) {
        log.info("update queue {} quota to {}, move window {}", queue, newQuota, moveWindow);
        quota.set(newQuota);
        recordCount.set(0);
    }

    @Override
    public void run() {
        log.info("QueueListener start...queue:{}", queue);
        while (RedisMqConsumerContainer.run) {
            try {
                if (recordCount.get() >= quota.get()) {
                    moveWindow.add(quota.get());
                    Thread.sleep(1000);
                    continue;
                }
                recordCount.getAndIncrement();
                Object msg = redisTemplate.opsForList().rightPop(queue, pollTimeout, TimeUnit.SECONDS);

                if (msg != null) {
                    try {
                        consumer.onMessage(msg);
                    } catch (Exception e) {
                        consumer.onError(msg, e);
                    }
                } else {
                    moveWindow.add(0);
                }
            } catch (QueryTimeoutException ignored) {
            } catch (Exception e) {
                if (RedisMqConsumerContainer.run) {
                    log.error("Queue: {}", queue, e);
                } else {
                    log.info("QueueListener exits...queue: {}, but container is destroyed", queue);
                }
            }
        }
    }

    public boolean isEmpty() {
        Long size = redisTemplate.opsForList().size(queue);
        log.info("queue size, {}, {}", queue, size);
        return null == size || 0L == size;
    }

    public long size() {
        Long size = redisTemplate.opsForList().size(queue);
        return null == size ? 0 : size;
    }

}
