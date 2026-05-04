package io.opencensus.trace.propagation;

/**
 * When Google's storage client library is loaded, it tries to set up distributed tracing
 * hooks — regardless of whether the application cares about tracing. These stub classes stand
 * in for the real tracing library so that setup succeeds silently, with all tracing calls
 * doing nothing.
 */
public final class SpanContextParseException extends Exception {
    public SpanContextParseException(String message) { super(message); }
    public SpanContextParseException(String message, Throwable cause) { super(message, cause); }
}
