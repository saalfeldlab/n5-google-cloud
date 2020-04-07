package org.janelia.saalfeldlab.googlecloud;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.IOException;

public abstract class GoogleCloudClient<T> {

	protected final Credentials credentials;

	public GoogleCloudClient(final Credentials credentials) {

		this.credentials = credentials;
	}

	public abstract T create();

	public static Credentials getSystemCredentials() throws IOException {

		return GoogleCredentials.getApplicationDefault();
	}
}
