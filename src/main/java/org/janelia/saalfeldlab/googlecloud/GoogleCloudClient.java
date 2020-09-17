/*-
 * #%L
 * N5 Google Cloud
 * %%
 * Copyright (C) 2017 - 2020 Igor Pisarev, Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
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
