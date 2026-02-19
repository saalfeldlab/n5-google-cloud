package org.janelia.saalfeldlab.googlecloud;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.janelia.saalfeldlab.n5.N5URI;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class GoogleCloudUtils {

	public final static Pattern GS_SCHEME = Pattern.compile("gs", Pattern.CASE_INSENSITIVE);
	public final static Pattern GS_HOST = Pattern.compile("(cloud\\.google|storage\\.googleapis)\\.com", Pattern.CASE_INSENSITIVE);

	private GoogleCloudUtils() {

	}

	public static String getGoogleCloudStorageKey(String uri) {

		return getGoogleCloudStorageKey(N5URI.getAsUri(uri));
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

        return createGoogleCloudStorage(googleCloudProjectId, opts -> {});
	}

	public static Storage createGoogleCloudStorage(
            @Nullable final String googleCloudProjectId,
            final Consumer<StorageOptions.Builder> builderConfig) {

        final String projectId = googleCloudProjectId != null ? googleCloudProjectId : StorageOptions.getDefaultProjectId();
        final StorageOptions.Builder builder = StorageOptions.newBuilder().setProjectId(projectId);

        builderConfig.accept(builder);

        return builder.build().getService();

    }
}
