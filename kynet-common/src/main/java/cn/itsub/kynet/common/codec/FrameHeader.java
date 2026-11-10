package cn.itsub.kynet.common.codec;

import cn.itsub.kynet.common.protocol.MessageType;

public final class FrameHeader {
    private static final MessageType[] MESSAGE_TYPES = MessageType.values();

    public final MessageType messageType;
    public final int action;
    public final int sourceInstanceId;
    public final int targetInstanceId;
    public final int topicHash;

    public FrameHeader(int messageTypeOrdinal, int action, int sourceInstanceId,
                       int targetInstanceId, int topicHash) {
        this.messageType = (messageTypeOrdinal >= 0 && messageTypeOrdinal < MESSAGE_TYPES.length)
                ? MESSAGE_TYPES[messageTypeOrdinal] : null;
        this.action = action;
        this.sourceInstanceId = sourceInstanceId;
        this.targetInstanceId = targetInstanceId;
        this.topicHash = topicHash;
    }
}
