package io.github.fakeyanss.example;

import io.github.fakeyanss.capacityawarequeueredis.core.consumer.MsgConsumer;
import lombok.extern.slf4j.Slf4j;

/**
 * description 此处自定义消费逻辑
 *
 * @author guichen01
 *         create date: 2020/8/11
 */
@Slf4j
public class CustomConsumer implements MsgConsumer {

    @Override
    public void onMessage(Object message) {
        // 随机发生错误
        int n = 1 / (((int) (Math.random() * 10)) / 2);
        log.info("收到消息:{}, id:{}", message, n);
    }

    @Override
    public void onError(Object msg, Exception e) {
        log.error("发生错误,消息:{}", msg, e);
    }

}
