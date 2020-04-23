package org.janelia.saalfeldlab.googlecloud;

import com.google.cloud.resourcemanager.ResourceManager;
import com.google.cloud.resourcemanager.ResourceManagerOptions;

public class GoogleCloudResourceManagerClient extends GoogleCloudClient<ResourceManager> {

	@Override
	public ResourceManager create() {

		return ResourceManagerOptions.newBuilder()
				.build().getService();
	}
}
