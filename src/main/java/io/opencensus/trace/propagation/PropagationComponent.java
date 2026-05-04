package io.opencensus.trace.propagation;

/**
 * When Google's storage client library is loaded, it tries to set up distributed tracing
 * hooks — regardless of whether the application cares about tracing. These stub classes stand
 * in for the real tracing library so that setup succeeds silently, with all tracing calls
 * doing nothing.
 */
public class PropagationComponent {

    public static final PropagationComponent NOOP = new PropagationComponent();

    PropagationComponent() {}

    public TextFormat getTextFormat() { return new TextFormat(); }

    public TextFormat getB3Format() { return new TextFormat(); }
}
