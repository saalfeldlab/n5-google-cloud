package org.janelia.saalfeldlab.googlecloud;

import com.google.auth.Credentials;
import com.google.cloud.resourcemanager.ResourceManager;
import com.google.cloud.resourcemanager.ResourceManagerOptions;

public class GoogleCloudResourceManagerClient extends GoogleCloudClient<ResourceManager> {

	public GoogleCloudResourceManagerClient(final Credentials credentials) {

		super(credentials);
	}

	@Override
	public ResourceManager create() {

		return ResourceManagerOptions.newBuilder()
				.setCredentials(credentials)
				.build().getService();
	}
}
