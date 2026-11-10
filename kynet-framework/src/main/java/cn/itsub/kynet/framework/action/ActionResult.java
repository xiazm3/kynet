package cn.itsub.kynet.framework.action;

import cn.itsub.kynet.common.codec.ProtobufPayloadCodec;

public class ActionResult {
    private final boolean success;
    private final String errorCode;
    private final String errorMessage;
    private final Object data;
    private final boolean attachmentUpdated;
    private final byte[] attachmentData;

    private ActionResult(boolean success, String errorCode, String errorMessage,
                         Object data, boolean attachmentUpdated, byte[] attachmentData) {
        this.success = success;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.data = data;
        this.attachmentUpdated = attachmentUpdated;
        this.attachmentData = attachmentData;
    }

    public static ActionResult success(Object data) {
        return new ActionResult(true, null, null, data, false, null);
    }

    public static ActionResult success(Object data, byte[] attachmentData) {
        return new ActionResult(true, null, null, data, true, attachmentData);
    }

    public static ActionResult success(Object data, byte[] attachmentData, boolean attachmentUpdated) {
        return new ActionResult(true, null, null, data, attachmentUpdated, attachmentData);
    }

    public static ActionResult error(String errorCode, String errorMessage) {
        return new ActionResult(false, errorCode, errorMessage, null, false, null);
    }

    public static ActionResult error(String errorCode, String errorMessage, byte[] attachmentData) {
        return new ActionResult(false, errorCode, errorMessage, null, true, attachmentData);
    }

    public static ActionResult error(String errorCode, String errorMessage,
                                     byte[] attachmentData, boolean attachmentUpdated) {
        return new ActionResult(false, errorCode, errorMessage, null, attachmentUpdated, attachmentData);
    }

    public boolean isSuccess() {
        return success;
    }

    public boolean getSuccess() {
        return success;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public Object getData() {
        return data;
    }

    public boolean isAttachmentUpdated() {
        return attachmentUpdated;
    }

    public byte[] getAttachmentData() {
        return attachmentData;
    }

    public <T> T getValue(Class<T> targetType) {
        if (data instanceof byte[] bytes) {
            return ProtobufPayloadCodec.decode(bytes, targetType);
        }
        return targetType.cast(data);
    }
}
