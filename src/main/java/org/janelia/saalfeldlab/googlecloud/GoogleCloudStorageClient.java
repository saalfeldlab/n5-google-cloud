package org.janelia.saalfeldlab.googlecloud;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class GoogleCloudStorageClient {

	public static enum StorageScope implements GoogleCloudOAuth.Scope {

		READ_ONLY("https://www.googleapis.com/auth/devstorage.read_only"),
		READ_WRITE("https://www.googleapis.com/auth/devstorage.read_write"),
		FULL_CONTROL("https://www.googleapis.com/auth/devstorage.full_control");

		private final String scope;

		private StorageScope(final String scope) {

			this.scope = scope;
		}

		@Override
		public String toString() {

			return scope;
		}
	}

	private final AccessToken accessToken;

	public GoogleCloudStorageClient(final AccessToken accessToken) {

		this.accessToken = accessToken;
	}

	public Storage create() {

		return createStorageClientBuilder().build().getService();
	}

	public Storage create(final String projectId) {

		return createStorageClientBuilder().setProjectId(projectId).build().getService();
	}

	private StorageOptions.Builder createStorageClientBuilder() {

		return StorageOptions.newBuilder().setCredentials(OAuth2Credentials.create(accessToken));
	}
}
