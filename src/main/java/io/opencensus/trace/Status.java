package io.opencensus.trace;

/**
 * When Google's storage client library is loaded, it tries to set up distributed tracing
 * hooks — regardless of whether the application cares about tracing. These stub classes stand
 * in for the real tracing library so that setup succeeds silently, with all tracing calls
 * doing nothing.
 */
public final class Status {

    public enum CanonicalCode {
        OK, CANCELLED, UNKNOWN, INVALID_ARGUMENT, DEADLINE_EXCEEDED,
        NOT_FOUND, ALREADY_EXISTS, PERMISSION_DENIED, UNAUTHENTICATED,
        RESOURCE_EXHAUSTED, FAILED_PRECONDITION, ABORTED, OUT_OF_RANGE,
        UNIMPLEMENTED, INTERNAL, UNAVAILABLE, DATA_LOSS;

        public Status toStatus() { return new Status(this, null); }
    }

    public static final Status OK                 = new Status(CanonicalCode.OK, null);
    public static final Status CANCELLED          = new Status(CanonicalCode.CANCELLED, null);
    public static final Status UNKNOWN            = new Status(CanonicalCode.UNKNOWN, null);
    public static final Status INVALID_ARGUMENT   = new Status(CanonicalCode.INVALID_ARGUMENT, null);
    public static final Status DEADLINE_EXCEEDED  = new Status(CanonicalCode.DEADLINE_EXCEEDED, null);
    public static final Status NOT_FOUND          = new Status(CanonicalCode.NOT_FOUND, null);
    public static final Status ALREADY_EXISTS     = new Status(CanonicalCode.ALREADY_EXISTS, null);
    public static final Status PERMISSION_DENIED  = new Status(CanonicalCode.PERMISSION_DENIED, null);
    public static final Status UNAUTHENTICATED    = new Status(CanonicalCode.UNAUTHENTICATED, null);
    public static final Status RESOURCE_EXHAUSTED = new Status(CanonicalCode.RESOURCE_EXHAUSTED, null);
    public static final Status FAILED_PRECONDITION= new Status(CanonicalCode.FAILED_PRECONDITION, null);
    public static final Status ABORTED            = new Status(CanonicalCode.ABORTED, null);
    public static final Status OUT_OF_RANGE       = new Status(CanonicalCode.OUT_OF_RANGE, null);
    public static final Status UNIMPLEMENTED      = new Status(CanonicalCode.UNIMPLEMENTED, null);
    public static final Status INTERNAL           = new Status(CanonicalCode.INTERNAL, null);
    public static final Status UNAVAILABLE        = new Status(CanonicalCode.UNAVAILABLE, null);
    public static final Status DATA_LOSS          = new Status(CanonicalCode.DATA_LOSS, null);

    private final CanonicalCode canonicalCode;
    private final String description;

    private Status(CanonicalCode canonicalCode, String description) {
        this.canonicalCode = canonicalCode;
        this.description = description;
    }

    public Status withDescription(String description) { return new Status(canonicalCode, description); }
    public CanonicalCode getCanonicalCode() { return canonicalCode; }
    public String getDescription() { return description; }
    public boolean isOk() { return canonicalCode == CanonicalCode.OK; }
}
