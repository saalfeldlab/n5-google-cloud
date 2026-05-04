package io.opencensus.trace;

import java.util.Map;

/**
 * When Google's storage client library is loaded, it tries to set up distributed tracing
 * hooks — regardless of whether the application cares about tracing. These stub classes stand
 * in for the real tracing library so that setup succeeds silently, with all tracing calls
 * doing nothing.
 */
public class Span {

    public enum Options { RECORD_EVENTS }

    public enum Kind { CLIENT, SERVER }

    protected Span() {}

    public void putAttribute(String key, AttributeValue value) {}
    public void putAttributes(Map<String, AttributeValue> attributes) {}
    public void addAttributes(Map<String, AttributeValue> attributes) {}
    public void addAnnotation(String description) {}
    public void addAnnotation(String description, Map<String, AttributeValue> attributes) {}
    public void addAnnotation(Annotation annotation) {}
    @Deprecated
    public void addNetworkEvent(NetworkEvent event) {}
    public void addMessageEvent(MessageEvent event) {}
    public void addLink(Link link) {}
    public void setStatus(Status status) {}
    public void end(EndSpanOptions options) {}
    public void end() {}

    public SpanContext getContext() { return SpanContext.INVALID; }
}
