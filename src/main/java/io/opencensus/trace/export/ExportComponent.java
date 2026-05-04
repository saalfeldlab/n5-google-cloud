package io.opencensus.trace.export;

/**
 * When Google's storage client library is loaded, it tries to set up distributed tracing
 * hooks — regardless of whether the application cares about tracing. These stub classes stand
 * in for the real tracing library so that setup succeeds silently, with all tracing calls
 * doing nothing.
 */
public class ExportComponent {

    public static final ExportComponent NOOP = new ExportComponent();

    ExportComponent() {}

    public SampledSpanStore getSampledSpanStore() { return new SampledSpanStore(); }
}
