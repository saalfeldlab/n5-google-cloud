package io.opencensus.common;

/**
 * When Google's storage client library is loaded, it tries to set up distributed tracing
 * hooks — regardless of whether the application cares about tracing. These stub classes stand
 * in for the real tracing library so that setup succeeds silently, with all tracing calls
 * doing nothing.
 */
public interface Scope {
    void close();
}
