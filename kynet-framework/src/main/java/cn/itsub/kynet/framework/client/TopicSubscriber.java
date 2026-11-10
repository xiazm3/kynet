package cn.itsub.kynet.framework.client;

@FunctionalInterface
public interface TopicSubscriber {
    void onMessage(String topic, byte[] payload);
}
