package io.opencensus.trace;

/**
 * When Google's storage client library is loaded, it tries to set up distributed tracing
 * hooks — regardless of whether the application cares about tracing. These stub classes stand
 * in for the real tracing library so that setup succeeds silently, with all tracing calls
 * doing nothing.
 */
public class MessageEvent extends BaseMessageEvent {

    public enum Type { SENT, RECEIVED }

    public static final class Builder {
        private Type type;
        private long messageId;
        private long uncompressedSize;
        private long compressedSize;

        Builder(Type type, long messageId) {
            this.type = type;
            this.messageId = messageId;
        }

        public Builder setUncompressedMessageSize(long size) { this.uncompressedSize = size; return this; }
        public Builder setCompressedMessageSize(long size)   { this.compressedSize = size;   return this; }

        public MessageEvent build() {
            return new MessageEvent(type, messageId, uncompressedSize, compressedSize);
        }
    }

    private final Type type;
    private final long messageId;
    private final long uncompressedSize;
    private final long compressedSize;

    private MessageEvent(Type type, long messageId, long uncompressedSize, long compressedSize) {
        this.type = type;
        this.messageId = messageId;
        this.uncompressedSize = uncompressedSize;
        this.compressedSize = compressedSize;
    }

    public static Builder builder(Type type, long messageId) { return new Builder(type, messageId); }

    public Type getType()                   { return type; }
    public long getMessageId()              { return messageId; }
    public long getUncompressedMessageSize(){ return uncompressedSize; }
    public long getCompressedMessageSize()  { return compressedSize; }
}
