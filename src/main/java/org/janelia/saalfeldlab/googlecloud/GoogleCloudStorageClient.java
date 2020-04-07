package org.janelia.saalfeldlab.googlecloud;

import com.google.auth.Credentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class GoogleCloudStorageClient extends GoogleCloudClient<Storage> {

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
