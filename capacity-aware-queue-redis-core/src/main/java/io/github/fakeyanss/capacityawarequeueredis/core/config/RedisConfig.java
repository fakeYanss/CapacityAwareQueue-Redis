package io.github.fakeyanss.capacityawarequeueredis.core.config;

import com.google.common.collect.Maps;

import io.github.fakeyanss.capacityawarequeueredis.core.consumer.MsgConsumer;
import io.github.fakeyanss.capacityawarequeueredis.core.consumer.RedisMqConsumerContainer;
import io.github.fakeyanss.capacityawarequeueredis.core.producer.RedisProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import javax.annotation.Resource;
import java.util.Map;

/**
 * redis 全局配置
 *
 * @author guichen01
 *         create date: 2020/8/11
 */
@Configuration
@Slf4j
public class RedisConfig {

    @Resource
    private ApplicationContext applicationContext;

    @Bean
    public RedisTemplate<Object, Object> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<Object, Object> redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(redisConnectionFactory);
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(new StringRedisSerializer());
        return redisTemplate;
    }

    @Resource
    private SchedulerConfiguration schedulerConfiguration;

    /**
     * 配置redis消息队列生产者
     *
     * @param redisTemplate redis
     * @return 生产者
     */
    @Bean
    public RedisProducer<Object> redisProducer(@Autowired RedisTemplate<Object, Object> redisTemplate) {
        return new RedisProducer<>(redisTemplate);
    }

    /**
     * 配置redis消息队列消费者容器
     */
    // @Bean(initMethod = "init", destroyMethod = "destroy")
    @Bean
    @ConditionalOnExpression("${scheduler.consumer.auto-register:true}")
    public RedisMqConsumerContainer redisMqConsumerContainer() {
        registerRedisMqConsumerContainer();
        return null;
        // RedisMqConsumerContainer container = new
        // RedisMqConsumerContainer(redisTemplate, schedulerConfiguration);
        // int maxPriority = schedulerConfiguration.getMaxPriority();
        // for (int i = 0; i < maxPriority; i++) {
        // String queueName = schedulerConfiguration.getDefaultTopic() + "-" + i;
        // container.addConsumer(QueueConfiguration.builder()
        // .queue(queueName)
        // .consumer(new CustomConsumer())
        // .priority(i)
        // .build());
        // }
        // return container;
    }

    /**
     * 根据配置项，动态创建消费容器。
     * 可以按照以下方法配置注册消费者容器，也可以自定义注册容器方法，
     * 如果只用一组公共队列，可以直接使用 {@link RedisMqConsumerContainer#addConsumer} 方法 \
     * 在 {@link RedisConfig#redisMqConsumerContainer} 注册容器
     */
    public void registerRedisMqConsumerContainer() {
        ConfigurableApplicationContext context = (ConfigurableApplicationContext) applicationContext;
        DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory) context.getAutowireCapableBeanFactory();
        Object redisTemplate = context.getBean("redisTemplate");
        int maxPriority = schedulerConfiguration.getMaxPriority();

        // 配置公共队列消费者容器
        Map<String, QueueConfiguration> consumerMap = buildQueueConfigurationMap(maxPriority,
                schedulerConfiguration.getDefaultTopic(), schedulerConfiguration.getConsumer().getCustomConsumerName());
        BeanDefinitionBuilder beanDefinitionBuilder = buildConsumerBean(schedulerConfiguration.getDefaultTopic(),
                redisTemplate, schedulerConfiguration, consumerMap);
        beanFactory.registerBeanDefinition("defaultRedisMqConsumerContainer",
                beanDefinitionBuilder.getBeanDefinition());
        // 调用context.getBean使注册生效
        log.info("{}", context.getBean("defaultRedisMqConsumerContainer"));

        // 配置大用户指定队列消费者容器
        schedulerConfiguration.getQueueMap().forEach((user, queue) -> {
            Map<String, QueueConfiguration> customConsumerMap = buildQueueConfigurationMap(maxPriority, queue,
                    schedulerConfiguration.getConsumer().getCustomConsumerName());
            BeanDefinitionBuilder customBeanDefinitionBuilder = buildConsumerBean(user, redisTemplate,
                    schedulerConfiguration, customConsumerMap);
            beanFactory.registerBeanDefinition("customRedisMqConsumerContainer" + "-" + user,
                    customBeanDefinitionBuilder.getBeanDefinition());
            // 调用context.getBean使注册生效
            log.info("{}", context.getBean("customRedisMqConsumerContainer" + "-" + user));
        });
    }

    private Map<String, QueueConfiguration> buildQueueConfigurationMap(int maxPriority, String logicTopic,
            String consumerName) {
        Map<String, QueueConfiguration> queueConfigurationMap = Maps.newHashMapWithExpectedSize(maxPriority);
        for (int i = 0; i < maxPriority; i++) {
            String queueName = logicTopic + "-" + i;
            try {
                queueConfigurationMap.put(queueName,
                        QueueConfiguration.builder()
                                .queue(queueName)
                                .consumer((MsgConsumer) Class.forName(consumerName).newInstance())
                                .priority(i)
                                .build());
            } catch (Exception e) {
                log.error("build queue configuration error, consumer name:{}", consumerName, e);
            }
        }
        return queueConfigurationMap;
    }

    private BeanDefinitionBuilder buildConsumerBean(String topic,
            Object redisTemplate,
            SchedulerConfiguration schedulerConfiguration,
            Map<String, QueueConfiguration> consumerMap) {
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.genericBeanDefinition(
                RedisMqConsumerContainer.class);
        beanDefinitionBuilder.addPropertyValue("logicQueueName", topic);
        beanDefinitionBuilder.addPropertyValue("redisTemplate", redisTemplate);
        beanDefinitionBuilder.addPropertyValue("schedulerConfiguration", schedulerConfiguration);
        beanDefinitionBuilder.addPropertyValue("consumerMap", consumerMap);
        beanDefinitionBuilder.setInitMethodName("init");
        beanDefinitionBuilder.setDestroyMethodName("destroy");
        return beanDefinitionBuilder;
    }

}
