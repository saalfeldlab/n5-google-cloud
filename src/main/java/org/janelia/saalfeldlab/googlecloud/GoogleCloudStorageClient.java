package org.janelia.saalfeldlab.googlecloud;

import com.google.auth.Credentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class GoogleCloudStorageClient extends GoogleCloudClient<Storage> {

	public static enum StorageScope implements Scope {

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

	private final String projectId;

	public GoogleCloudStorageClient(final Credentials credentials) {

		this(credentials, null);
	}

	public GoogleCloudStorageClient(final Credentials credentials, final String projectId) {

		super(credentials);
		this.projectId = projectId;
	}

	@Override
	public Storage create() {

		return StorageOptions.newBuilder()
				.setCredentials(credentials)
				.setProjectId(projectId)
				.build().getService();
	}
}
