package org.janelia.saalfeldlab.googlecloud;

import java.lang.reflect.Field;
import java.util.Map;

public abstract class GoogleCloudClient<T> {

	public abstract T create();
}
