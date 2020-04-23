package org.janelia.saalfeldlab.googlecloud;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class GoogleCloudStorageClient extends GoogleCloudClient<Storage> {

	private final String projectId;

	public GoogleCloudStorageClient() {

		this(null);
	}

	public GoogleCloudStorageClient(final String projectId) {

		this.projectId = projectId;
	}

	@Override
	public Storage create() {

		return StorageOptions.newBuilder()
				.setProjectId(projectId)
				.build().getService();
	}
}
