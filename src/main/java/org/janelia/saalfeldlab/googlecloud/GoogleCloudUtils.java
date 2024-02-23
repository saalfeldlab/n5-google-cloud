package org.janelia.saalfeldlab.googlecloud;

import com.google.cloud.resourcemanager.Project;
import com.google.cloud.resourcemanager.ResourceManager;
import com.google.cloud.storage.Storage;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.regex.Pattern;

public class GoogleCloudUtils {

	public final static Pattern GS_SCHEME = Pattern.compile("gs", Pattern.CASE_INSENSITIVE);
	public final static Pattern GS_HOST = Pattern.compile("(cloud\\.google|storage\\.googleapis)\\.com", Pattern.CASE_INSENSITIVE);

	private GoogleCloudUtils() {

	}

	public static Storage createGoogleCloudStorage(@Nullable final String googleCloudProjectId) {

		final GoogleCloudStorageClient storageClient = getGoogleCloudStorageClient(googleCloudProjectId);
		if (storageClient == null)
			return null;

		return storageClient.create();
	}

	private static GoogleCloudStorageClient getGoogleCloudStorageClient(@Nullable final String googleCloudProjectId) {

		final String projectId;
		if (googleCloudProjectId == null) {
			final ResourceManager resourceManager = new GoogleCloudResourceManagerClient().create();
			final Iterator<Project> projectsIterator = resourceManager.list().iterateAll().iterator();
			projectId = projectsIterator.hasNext() ? projectsIterator.next().getProjectId() : null;
		} else
			projectId = googleCloudProjectId;
		return new GoogleCloudStorageClient(projectId);
	}
}
