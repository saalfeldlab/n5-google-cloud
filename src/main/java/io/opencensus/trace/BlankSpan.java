package io.opencensus.trace;

import java.util.Map;

/**
 * When Google's storage client library is loaded, it tries to set up distributed tracing
 * hooks — regardless of whether the application cares about tracing. These stub classes stand
 * in for the real tracing library so that setup succeeds silently, with all tracing calls
 * doing nothing.
 */
public final class BlankSpan extends Span {

    public static final BlankSpan INSTANCE = new BlankSpan();

    private BlankSpan() {}

    @Override public void putAttribute(String key, AttributeValue value) {}
    @Override public void putAttributes(Map<String, AttributeValue> attributes) {}
    @Override public void addAnnotation(String description, Map<String, AttributeValue> attributes) {}
    @Override public void addAnnotation(Annotation annotation) {}
    @Override public void addMessageEvent(MessageEvent event) {}
    @Override public void addLink(Link link) {}
    @Override public void setStatus(Status status) {}
    @Override public void end(EndSpanOptions options) {}

    @Override
    public String toString() { return "BlankSpan"; }
}
