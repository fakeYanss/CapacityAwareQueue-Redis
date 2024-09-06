package io.github.fakeyanss.example;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import io.github.fakeyanss.capacityawarequeueredis.core.config.SchedulerConfiguration;
import io.github.fakeyanss.capacityawarequeueredis.core.producer.RedisProducer;

import javax.annotation.Resource;

/**
 * description
 *
 * @author guichen01
 *         create date: 2020/8/11
 */
@RestController
public class TestController {

    @Resource
    private RedisProducer<String> redisProducer;
    @Resource
    private SchedulerConfiguration schedulerConfiguration;

    @GetMapping()
    public String home() {
        return "halo";
    }

    @PostMapping("/send/{priority}")
    public void sendMsg(@PathVariable int priority) {
        for (int i = 0; i < 10; i++) {
            redisProducer.sendMsg(schedulerConfiguration.getDefaultTopic(), priority, "halo, priority " + priority);
            redisProducer.sendMsg(schedulerConfiguration.getQueueMap().get("vip-user-1"), priority,
                    "halo, priority " + priority);
        }
    }

}
