package org.janelia.saalfeldlab.googlecloud;

import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.auth.oauth2.AccessToken;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class GoogleCloudStorageClient extends GoogleCloudClient {

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

	public GoogleCloudStorageClient(final AccessToken accessToken) {

		super(accessToken);
	}

	public GoogleCloudStorageClient(final AccessToken accessToken, final GoogleClientSecrets clientSecrets, final String refreshToken) {

		super(accessToken, clientSecrets, refreshToken);
	}

	public Storage create() {

		return createStorageClientBuilder().build().getService();
	}

	public Storage create(final String projectId) {

		return createStorageClientBuilder().setProjectId(projectId).build().getService();
	}

	private StorageOptions.Builder createStorageClientBuilder() {

		return StorageOptions.newBuilder().setCredentials(getCredentials());
	}
}
