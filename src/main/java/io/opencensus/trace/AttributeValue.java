package io.opencensus.trace;

import io.opencensus.common.Function;

/**
 * When Google's storage client library is loaded, it tries to set up distributed tracing
 * hooks — regardless of whether the application cares about tracing. These stub classes stand
 * in for the real tracing library so that setup succeeds silently, with all tracing calls
 * doing nothing.
 */
public class AttributeValue {

    private final Object value;

    AttributeValue(Object value) { this.value = value; }

    public static AttributeValue stringAttributeValue(String value) { return new AttributeValue(value); }
    public static AttributeValue booleanAttributeValue(boolean value) { return new AttributeValue(value); }
    public static AttributeValue longAttributeValue(long value) { return new AttributeValue(value); }
    public static AttributeValue doubleAttributeValue(double value) { return new AttributeValue(value); }

    public <T> T match(Function<? super String, T> p0, Function<? super Boolean, T> p1,
            Function<? super Long, T> p2, Function<Object, T> defaultFn) { return null; }

    public <T> T match(Function<? super String, T> p0, Function<? super Boolean, T> p1,
            Function<? super Long, T> p2, Function<? super Double, T> p3,
            Function<Object, T> defaultFn) { return null; }
}
