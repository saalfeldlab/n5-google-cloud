package io.opencensus.trace.propagation;

import io.opencensus.trace.SpanContext;
import java.util.Collections;
import java.util.List;

/**
 * When Google's storage client library is loaded, it tries to set up distributed tracing
 * hooks — regardless of whether the application cares about tracing. These stub classes stand
 * in for the real tracing library so that setup succeeds silently, with all tracing calls
 * doing nothing.
 */
public class TextFormat {

    public abstract static class Setter<C> {
        public abstract void put(C carrier, String key, String value);
    }

    public abstract static class Getter<C> {
        public abstract String get(C carrier, String key);
    }

    public List<String> fields() { return Collections.emptyList(); }

    public <C> void inject(SpanContext spanContext, C carrier, Setter<C> setter) {}

    public <C> SpanContext extract(C carrier, Getter<C> getter)
            throws SpanContextParseException {
        return SpanContext.INVALID;
    }
}
