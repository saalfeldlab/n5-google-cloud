package io.opencensus.trace;

import io.opencensus.trace.export.ExportComponent;
import io.opencensus.trace.propagation.PropagationComponent;

/**
 * When Google's storage client library is loaded, it tries to set up distributed tracing
 * hooks — regardless of whether the application cares about tracing. These stub classes stand
 * in for the real tracing library so that setup succeeds silently, with all tracing calls
 * doing nothing.
 */
public final class Tracing {

    private static final Tracer tracer = Tracer.NOOP;
    private static final PropagationComponent propagationComponent = PropagationComponent.NOOP;
    private static final ExportComponent exportComponent = ExportComponent.NOOP;

    private Tracing() {}

    public static Tracer getTracer() { return tracer; }

    public static PropagationComponent getPropagationComponent() { return propagationComponent; }

    public static ExportComponent getExportComponent() { return exportComponent; }
}
