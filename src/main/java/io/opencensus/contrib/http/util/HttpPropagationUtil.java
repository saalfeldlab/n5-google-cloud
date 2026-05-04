package io.opencensus.contrib.http.util;

import io.opencensus.trace.propagation.TextFormat;

/**
 * When Google's storage client library is loaded, it tries to set up distributed tracing
 * hooks — regardless of whether the application cares about tracing. These stub classes stand
 * in for the real tracing library so that setup succeeds silently, with all tracing calls
 * doing nothing.
 */
public final class HttpPropagationUtil {

    private HttpPropagationUtil() {}

    public static TextFormat getCloudTraceFormat() { return new TextFormat(); }

    public static TextFormat getB3Format() { return new TextFormat(); }
}
