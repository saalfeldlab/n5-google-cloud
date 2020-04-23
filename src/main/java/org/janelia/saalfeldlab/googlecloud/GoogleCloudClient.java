package org.janelia.saalfeldlab.googlecloud;

import java.lang.reflect.Field;
import java.util.Map;

public abstract class GoogleCloudClient<T> {

	public GoogleCloudClient() {

		suppressCredentialsWarning();
	}

	public abstract T create();

	/**
	 * Google Cloud SDK prints a warning about authenticating using end user credentials instead of service accounts.
	 *
	 * While this makes sense for running the code on Google Cloud itself, there is nothing wrong with using
	 * end user credentials on a local machine generated with 'gcloud auth'.
	 *
	 * To suppress this warning, an environment variable needs to be set. This method automates it by setting
	 * the environment variable with reflection.
	 */
	@SuppressWarnings({ "unchecked" })
	private static void suppressCredentialsWarning() {

		try {
			final Map<String, String> env = System.getenv();
			final Field field = env.getClass().getDeclaredField("m");
			field.setAccessible(true);
			((Map<String, String>) field.get(env)).put(SUPPRESS_GCLOUD_CREDS_WARNING_ENV_VAR, Boolean.TRUE.toString());
		} catch (final ReflectiveOperationException e) {
			e.printStackTrace();
		}
	}
	private static final String SUPPRESS_GCLOUD_CREDS_WARNING_ENV_VAR = "SUPPRESS_GCLOUD_CREDS_WARNING";
}
