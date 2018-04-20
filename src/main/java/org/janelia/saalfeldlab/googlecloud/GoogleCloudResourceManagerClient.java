package org.janelia.saalfeldlab.googlecloud;

import com.google.auth.Credentials;
import com.google.cloud.resourcemanager.ResourceManager;
import com.google.cloud.resourcemanager.ResourceManagerOptions;

public class GoogleCloudResourceManagerClient extends GoogleCloudClient<ResourceManager> {

	public static enum ProjectsScope implements Scope {

		READ_ONLY("https://www.googleapis.com/auth/cloudplatformprojects.readonly"),
		FULL_CONTROL("https://www.googleapis.com/auth/cloudplatformprojects");

		private final String scope;

		private ProjectsScope(final String scope) {

			this.scope = scope;
		}

		@Override
		public String toString() {

			return scope;
		}
	}

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
