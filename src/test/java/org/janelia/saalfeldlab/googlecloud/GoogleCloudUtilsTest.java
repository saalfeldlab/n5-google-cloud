package org.janelia.saalfeldlab.googlecloud;

import org.junit.Test;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class GoogleCloudUtilsTest {

	@Test
	public void getGSInfoTest() {

		final String bucketName = "my-gs-bucket";
		final String[] noKeyTests = new String[]{
				"https://googleapis.com/storage/v1/b/" + bucketName,
				"https://www.googleapis.com/storage/v1/b/" + bucketName,
				"https://storage.cloud.google.com/" + bucketName,
				"gs://" + bucketName,
		};

		final String[] rootKeyTests = new String[]{
				"http://googleapis.com/storage/v1/b/" + bucketName + "/",
				"http://www.googleapis.com/storage/v1/b/" + bucketName + "/",
				"http://storage.cloud.google.com/" + bucketName + "/",
				"gs://" + bucketName + "/",
		};

		final String[] onePartKeyWithSlashTests = new String[]{
				"http://googleapis.com/storage/v1/b/" + bucketName + "/a/",
				"http://www.googleapis.com/storage/v1/b/" + bucketName + "/a/",
				"http://storage.cloud.google.com/" + bucketName + "/a/",
				"gs://" + bucketName + "/a/",
		};

		final String[] multiPartKeyWithSlashTests = new String[]{
				"http://googleapis.com/storage/v1/b/" + bucketName + "/a/b/c/d/",
				"http://www.googleapis.com/storage/v1/b/" + bucketName + "/a/b/c/d/",
				"http://storage.cloud.google.com/" + bucketName + "/a/b/c/d/",
				"gs://" + bucketName + "/a/b/c/d/",
		};

		final String[] onePartKeyNoSlashTests = new String[]{
				"http://googleapis.com/storage/v1/b/" + bucketName + "/a",
				"http://www.googleapis.com/storage/v1/b/" + bucketName + "/a",
				"http://storage.cloud.google.com/" + bucketName + "/a",
				"gs://" + bucketName + "/a",
		};

		final String[] multiPartKeyNoSlashTests = new String[]{
				"http://googleapis.com/storage/v1/b/" + bucketName + "/a/b/c/d",
				"http://www.googleapis.com/storage/v1/b/" + bucketName + "/a/b/c/d",
				"http://storage.cloud.google.com/" + bucketName + "/a/b/c/d",
				"gs://" + bucketName + "/a/b/c/d",
		};

		final HashMap<String, String[]> keyToTests = new HashMap<>();

		keyToTests.put("", noKeyTests);
		keyToTests.put("/", rootKeyTests);
		keyToTests.put("/a/", onePartKeyWithSlashTests);
		keyToTests.put("/a/b/c/d/", multiPartKeyWithSlashTests);
		keyToTests.put("/a", onePartKeyNoSlashTests);
		keyToTests.put("/a/b/c/d", multiPartKeyNoSlashTests);

		for (Map.Entry<String, String[]> tests : keyToTests.entrySet()) {
			final String expectedKey = tests.getKey();
			for (String uri : tests.getValue()) {
				final GoogleCloudStorageURI gsUri;
				try {
					gsUri = new GoogleCloudStorageURI(uri);
				} catch (Throwable e) {
					System.err.println("Could not parse Google Cloud URI for " + uri);
					throw e;
				}
				assertEquals(bucketName, gsUri.getBucket());
				assertEquals("Unexpected key for " + uri, expectedKey, GoogleCloudUtils.getGoogleCloudStorageKey(uri));
			}
		}

		assertThrows("Invalid URI should throw exception", Throwable.class, () -> new GoogleCloudStorageURI(("invalid uri \\ _ ~ 435:  q2234[;5.")));
		assertThrows("Invalid URI should throw exception", Throwable.class, () -> GoogleCloudUtils.getGoogleCloudStorageKey("invalid uri \\ _ ~ 435:  q2234[;5."));
	}
}