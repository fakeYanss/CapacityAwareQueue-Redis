package io.github.fakeyanss.capacityawarequeueredis.core.consumer;

public interface MsgConsumer {

    void onMessage(Object message);

    void onError(Object msg, Exception e);

}
