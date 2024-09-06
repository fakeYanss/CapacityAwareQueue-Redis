package io.github.fakeyanss.capacityawarequeueredis.core.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * description
 *
 * @author guichen01
 *         create date: 2020/8/13
 */
@ConfigurationProperties(prefix = "scheduler")
@Configuration
@Data
public class SchedulerConfiguration {

    private String defaultTopic;
    private Map<String, String> queueMap;
    private int maxPriority;
    private ConsumerConfiguration consumer;

}
