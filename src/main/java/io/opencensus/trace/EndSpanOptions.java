package io.opencensus.trace;

/**
 * When Google's storage client library is loaded, it tries to set up distributed tracing
 * hooks — regardless of whether the application cares about tracing. These stub classes stand
 * in for the real tracing library so that setup succeeds silently, with all tracing calls
 * doing nothing.
 */
public class EndSpanOptions {

    public static final EndSpanOptions DEFAULT = new EndSpanOptions(false, null);

    public static final class Builder {
        private boolean sampleToLocalSpanStore;
        private Status status;

        Builder() {}

        public Builder setSampleToLocalSpanStore(boolean sample) {
            this.sampleToLocalSpanStore = sample; return this;
        }

        public Builder setStatus(Status status) {
            this.status = status; return this;
        }

        public EndSpanOptions build() {
            return new EndSpanOptions(sampleToLocalSpanStore, status);
        }
    }

    private final boolean sampleToLocalSpanStore;
    private final Status status;

    private EndSpanOptions(boolean sampleToLocalSpanStore, Status status) {
        this.sampleToLocalSpanStore = sampleToLocalSpanStore;
        this.status = status;
    }

    public static Builder builder() { return new Builder(); }

    public boolean getSampleToLocalSpanStore() { return sampleToLocalSpanStore; }
    public Status getStatus() { return status; }
}
