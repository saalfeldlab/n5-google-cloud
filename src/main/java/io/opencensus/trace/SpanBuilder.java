package io.opencensus.trace;

import io.opencensus.common.Scope;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * When Google's storage client library is loaded, it tries to set up distributed tracing
 * hooks — regardless of whether the application cares about tracing. These stub classes stand
 * in for the real tracing library so that setup succeeds silently, with all tracing calls
 * doing nothing.
 */
public class SpanBuilder {

    static final SpanBuilder NOOP = new SpanBuilder();

    SpanBuilder() {}

    public SpanBuilder setSampler(Sampler sampler)           { return this; }
    public SpanBuilder setParentLinks(List<Span> parentLinks){ return this; }
    public SpanBuilder setRecordEvents(boolean recordEvents) { return this; }
    public SpanBuilder setSpanKind(Span.Kind kind)           { return this; }

    public Span startSpan() { return BlankSpan.INSTANCE; }

    public Scope startScopedSpan() {
        return new Scope() { public void close() {} };
    }

    public void startSpanAndRun(Runnable runnable) { runnable.run(); }

    public <V> V startSpanAndCall(Callable<V> callable) throws Exception {
        return callable.call();
    }
}
