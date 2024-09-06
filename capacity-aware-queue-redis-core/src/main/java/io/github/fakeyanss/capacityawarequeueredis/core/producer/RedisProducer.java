package io.github.fakeyanss.capacityawarequeueredis.core.producer;

import org.springframework.data.redis.core.RedisTemplate;

/**
 * description
 *
 * @author guichen01
 *         create date: 2020/8/11
 */
public class RedisProducer<T> {

    private RedisTemplate<Object, T> redisTemplate;

    public RedisProducer(RedisTemplate<Object, T> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void sendMsg(String queue, int priority, T msg) {
        redisTemplate.opsForList().leftPush(queue + "-" + priority, msg);
    }

}
