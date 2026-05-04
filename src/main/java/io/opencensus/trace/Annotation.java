package io.opencensus.trace;

import java.util.Collections;
import java.util.Map;

/**
 * When Google's storage client library is loaded, it tries to set up distributed tracing
 * hooks — regardless of whether the application cares about tracing. These stub classes stand
 * in for the real tracing library so that setup succeeds silently, with all tracing calls
 * doing nothing.
 */
public class Annotation {

    private final String description;
    private final Map<String, AttributeValue> attributes;

    Annotation(String description, Map<String, AttributeValue> attributes) {
        this.description = description;
        this.attributes = attributes;
    }

    public static Annotation fromDescription(String description) {
        return new Annotation(description, Collections.<String, AttributeValue>emptyMap());
    }

    public static Annotation fromDescriptionAndAttributes(String description,
            Map<String, AttributeValue> attributes) {
        return new Annotation(description, attributes);
    }

    public String getDescription() { return description; }
    public Map<String, AttributeValue> getAttributes() { return attributes; }
}
