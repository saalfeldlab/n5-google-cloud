package io.opencensus.trace;

/**
 * When Google's storage client library is loaded, it tries to set up distributed tracing
 * hooks — regardless of whether the application cares about tracing. These stub classes stand
 * in for the real tracing library so that setup succeeds silently, with all tracing calls
 * doing nothing.
 */
public final class SpanContext {
    public static final SpanContext INVALID = new SpanContext();

    private SpanContext() {}

    public boolean isValid() { return false; }

    @Override
    public String toString() { return "SpanContext{INVALID}"; }
}
