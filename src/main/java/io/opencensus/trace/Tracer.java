package io.opencensus.trace;

import io.opencensus.common.Scope;
import java.util.concurrent.Callable;

/**
 * When Google's storage client library is loaded, it tries to set up distributed tracing
 * hooks — regardless of whether the application cares about tracing. These stub classes stand
 * in for the real tracing library so that setup succeeds silently, with all tracing calls
 * doing nothing.
 */
public class Tracer {

    static final Tracer NOOP = new Tracer();

    protected Tracer() {}

    public Span getCurrentSpan() { return BlankSpan.INSTANCE; }

    public Scope withSpan(Span span) {
        return new Scope() { public void close() {} };
    }

    public Runnable withSpan(Span span, Runnable runnable) { return runnable; }

    public <C> Callable<C> withSpan(Span span, Callable<C> callable) { return callable; }

    public SpanBuilder spanBuilder(String spanName) {
        return SpanBuilder.NOOP;
    }

    public SpanBuilder spanBuilderWithExplicitParent(String spanName, Span parent) {
        return SpanBuilder.NOOP;
    }

    public SpanBuilder spanBuilderWithRemoteParent(String spanName, SpanContext remoteParent) {
        return SpanBuilder.NOOP;
    }
}
