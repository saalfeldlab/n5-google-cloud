package org.janelia.saalfeldlab.googlecloud;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.regex.Pattern;

public class GoogleCloudUtils {

	public final static Pattern GS_SCHEME = Pattern.compile("gs", Pattern.CASE_INSENSITIVE);
	public final static Pattern GS_HOST = Pattern.compile("(cloud\\.google|storage\\.googleapis)\\.com", Pattern.CASE_INSENSITIVE);

	private GoogleCloudUtils() {

	}

	public static String getGoogleCloudStorageKey(String uri) {

		return getGoogleCloudStorageKey(URI.create(uri));
	}

	public static String getGoogleCloudStorageKey(URI uri) {

		try {
			// if key is null, return the empty string
			final String key =  new GoogleCloudStorageURI(uri).getKey();
			return key == null ? "" : key;
		} catch (final Exception e) {
		}
		// parse key manually when GoogleCLoudStorageURI can't
		final String path = uri.getPath().replaceFirst("^/", "");
		return path.substring(path.indexOf('/') + 1);
	}

	public static Storage createGoogleCloudStorage(@Nullable final String googleCloudProjectId) {

		final GoogleCloudStorageClient storageClient = getGoogleCloudStorageClient(googleCloudProjectId);
		if (storageClient == null)
			return null;

		return storageClient.create();
	}

	public static GoogleCloudStorageClient getGoogleCloudStorageClient(@Nullable final String googleCloudProjectId) {

		return new GoogleCloudStorageClient(googleCloudProjectId != null ? googleCloudProjectId :
				StorageOptions.getDefaultProjectId());

	}
}
