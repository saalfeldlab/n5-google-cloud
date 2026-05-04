package io.opencensus.trace;

import java.util.Collections;
import java.util.Map;

/**
 * When Google's storage client library is loaded, it tries to set up distributed tracing
 * hooks — regardless of whether the application cares about tracing. These stub classes stand
 * in for the real tracing library so that setup succeeds silently, with all tracing calls
 * doing nothing.
 */
public class Link {

    public enum Type { CHILD_LINKED_SPAN, PARENT_LINKED_SPAN }

    private Link() {}

    public static Link fromSpanContext(SpanContext context, Type type) {
        return new Link();
    }

    public static Link fromSpanContext(SpanContext context, Type type,
            Map<String, AttributeValue> attributes) {
        return new Link();
    }

    public Type getType() { return null; }
    public Map<String, AttributeValue> getAttributes() {
        return Collections.<String, AttributeValue>emptyMap();
    }
}
