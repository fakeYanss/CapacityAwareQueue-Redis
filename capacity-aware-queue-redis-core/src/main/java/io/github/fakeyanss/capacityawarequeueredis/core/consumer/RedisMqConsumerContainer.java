package io.github.fakeyanss.capacityawarequeueredis.core.consumer;

import com.google.common.collect.Lists;

import io.github.fakeyanss.capacityawarequeueredis.core.config.QueueConfiguration;
import io.github.fakeyanss.capacityawarequeueredis.core.config.SchedulerConfiguration;
import io.github.fakeyanss.capacityawarequeueredis.core.consumer.dynamicadjuster.ExpMaxPollDistributor;
import io.github.fakeyanss.capacityawarequeueredis.core.consumer.dynamicadjuster.MoveWindow;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 消费层容器，负责创建消费线程，并管理消费线程的quota
 *
 * @author guichen01
 *         create date: 2020/8/11
 */
@Slf4j
@NoArgsConstructor
@Data
public class RedisMqConsumerContainer {

    private String logicQueueName;
    private Map<String, QueueConfiguration> consumerMap = new HashMap<>();
    private RedisTemplate<Object, Object> redisTemplate;
    private SchedulerConfiguration schedulerConfiguration;
    static boolean run;
    private ExecutorService consumerPool;
    private ExecutorService manager;

    public RedisMqConsumerContainer(String logicQueueName, RedisTemplate<Object, Object> redisTemplate,
            SchedulerConfiguration schedulerConfiguration) {
        this.logicQueueName = logicQueueName;
        this.redisTemplate = redisTemplate;
        this.schedulerConfiguration = schedulerConfiguration;
    }

    public void addConsumer(QueueConfiguration configuration) {
        if (consumerMap.containsKey(configuration.getQueue())) {
            log.warn("Key:{} this key already exists, and it will be replaced", configuration.getQueue());
        }
        if (configuration.getConsumer() == null) {
            log.warn("Key:{} consumer cannot be null, this configuration will be skipped", configuration.getQueue());
        }
        consumerMap.put(configuration.getQueue(), configuration);
    }

    public void destroy() {
        run = false;
        this.consumerPool.shutdown();
        this.manager.shutdown();
        log.info("QueueListener exiting.");
        while (!this.consumerPool.isTerminated() || !manager.isTerminated()) {
        }
        log.info("QueueListener exited.");
    }

    public void init() {
        log.debug("config:::{}", schedulerConfiguration);
        run = true;
        AtomicInteger threadNumber = new AtomicInteger(1);
        this.consumerPool = Executors.newFixedThreadPool(schedulerConfiguration.getMaxPriority(),
                r -> new Thread(r, "RedisMQListener-" + logicQueueName + "-" + threadNumber.getAndIncrement()));
        consumerMap = Collections.unmodifiableMap(consumerMap);
        List<QueueListener> queueListeners = Lists.newArrayListWithExpectedSize(consumerMap.size());

        Map<Integer, Integer> defaultQuotaMap = ExpMaxPollDistributor.instance().distribution(
                schedulerConfiguration.getMaxPriority(), schedulerConfiguration.getConsumer().getMaxPollRecords());

        Map<Integer, Integer> finalQuotaMap = ExpMaxPollDistributor.instance().distribution(
                schedulerConfiguration.getMaxPriority(), schedulerConfiguration.getConsumer().getMaxPollRecords());

        consumerMap.forEach((k, v) -> queueListeners.add(
                QueueListener.builder()
                        .redisTemplate(redisTemplate)
                        .queue(v.getQueue())
                        .consumer(v.getConsumer())
                        .priority(v.getPriority())
                        .quota(new AtomicInteger(defaultQuotaMap.get(v.getPriority())))
                        .recordCount(new AtomicInteger(0))
                        .moveWindow(new MoveWindow(
                                schedulerConfiguration.getConsumer().getMaxPollHistoryWindowSize(),
                                schedulerConfiguration.getConsumer().getMinPollWindowMaxOutThreshold(),
                                defaultQuotaMap.get(v.getPriority())))
                        .pollTimeout(schedulerConfiguration.getConsumer().getPollTimeout())
                        .build()));

        queueListeners.forEach(q -> consumerPool.submit(q));

        // 启动管理线程，按优先级动态调整队列消费数
        manager = Executors.newSingleThreadExecutor(r -> new Thread(r, "RedisMQManager-" + logicQueueName));
        manager.submit(() -> {
            while (run) {
                int priority = -1;
                try {
                    Thread.sleep(1000);
                    priority = adjust(queueListeners, defaultQuotaMap, finalQuotaMap);
                    queueListeners.forEach(q -> {
                        if (q.getRecordCount() >= q.getQuota().get()) {
                            q.resetQuota(finalQuotaMap.get(q.getPriority()));
                        }
                    });
                } catch (Exception e) {
                    log.error("consumer daemon thread error", e);
                } finally {
                    if (priority >= 0) {
                        // 还原quota map
                        finalQuotaMap.put(priority, defaultQuotaMap.get(priority));
                    }
                }
            }
        });

    }

    // 从高优队列开始调整quota，每次只能burst一个队列
    private int adjust(List<QueueListener> queueListeners, Map<Integer, Integer> quotaMap,
            Map<Integer, Integer> finalQuotaMap) {
        for (int i = queueListeners.size() - 1; i >= 0; i--) {
            QueueListener q = queueListeners.get(i);
            if (q.getRecordCount() >= q.getQuota().get()) {
                int burst = quotaMap.get(q.getPriority());
                if (q.getMoveWindow().isMaxedOutThresholdBreach()) {
                    int sum = 0;
                    for (QueueListener q1 : queueListeners) {
                        if (q1.getPriority() != q.getPriority()) {
                            int maxUnusedValue = q1.getMoveWindow().maxUnusedValue();
                            sum += maxUnusedValue;
                        }
                    }
                    burst += sum;
                    finalQuotaMap.put(q.getPriority(), burst);
                    return i;
                }
            }
        }
        return -1;
    }

}
