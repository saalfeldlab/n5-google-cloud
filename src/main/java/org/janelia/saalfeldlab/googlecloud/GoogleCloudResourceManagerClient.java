package org.janelia.saalfeldlab.googlecloud;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.AccessToken;
import com.google.cloud.resourcemanager.Project;
import com.google.cloud.resourcemanager.ResourceManager;
import com.google.cloud.resourcemanager.ResourceManagerOptions;

public class GoogleCloudResourceManagerClient extends GoogleCloudClient {

	public static enum ProjectsScope implements GoogleCloudOAuth.Scope {

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

	public GoogleCloudResourceManagerClient(final AccessToken accessToken) {

		super(accessToken);
	}

	public GoogleCloudResourceManagerClient(final AccessToken accessToken, final GoogleClientSecrets clientSecrets, final String refreshToken) {

		super(accessToken, clientSecrets, refreshToken);
	}

	public ResourceManager create() {

		return ResourceManagerOptions.newBuilder().setCredentials(getCredentials()).build().getService();
	}

	public static List<String> listProjects(final ResourceManager resourceManager) {

		final List<String> projectIds = new ArrayList<>();
		final Page<Project> projectsListing = resourceManager.list();
		for (final Iterator<Project> projectIterator = projectsListing.iterateAll().iterator(); projectIterator.hasNext();)
			projectIds.add(projectIterator.next().getProjectId());
		return projectIds;
	}
}
