package org.janelia.saalfeldlab.n5.googlecloud.backend;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;

import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.googlecloud.GcsIoPolicy;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.security.SecureRandom;

public class BackendIoPolicyTests {

	private static final SecureRandom random = new SecureRandom();

	protected static Storage storage;
	protected static String bucketName;

	@BeforeClass
	public static void setup() {

		storage = BackendGoogleCloudStorageFactory.getOrCreateStorage();
		bucketName = "n5-test-" + Long.toUnsignedString(random.nextLong());
		storage.create(BucketInfo.of(bucketName));
	}

	@AfterClass
	public static void teardown() {

		Page<Blob> page = storage.list(bucketName, Storage.BlobListOption.fields(Storage.BlobField.ID));
		while (page != null) {
			final BlobId[] ids = page.streamValues().map(Blob::getBlobId).toArray(BlobId[]::new);
			if (ids.length > 0)
				storage.delete(ids);
			page = page.getNextPage();
		}
		storage.delete(bucketName);
	}

	@Test
	public void testUnsafe() throws IOException {

		final GcsIoPolicy.Unsafe policy = new GcsIoPolicy.Unsafe(storage, bucketName);
		final byte[] data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
		final byte[] data2 = {10, 11, 12};

		// write and full read roundtrip
		policy.write("unsafe/blob", ReadData.from(data));
		try (VolatileReadData result = policy.read("unsafe/blob")) {
			assertArrayEquals(data, result.allBytes());
		}

		// concurrent modification: 
		// overwrite the blob, then attempt to materialize, shoud succeed
		try (VolatileReadData vrd = policy.read("genmatch/blob")) {

			// should not store the generation
			vrd.requireLength();

			// overwrite to new generation
			policy.write("genmatch/blob", ReadData.from(data2));

			// ensure fetching the data gets the new data and does not error
			assertArrayEquals(data2, vrd.allBytes());
		}
	}

	@Test
	public void testGenerationMatch() throws IOException {

		final GcsIoPolicy.GenerationMatch policy = new GcsIoPolicy.GenerationMatch(storage, bucketName);
		final byte[] data1 = {0, 1, 2, 3, 4};
		final byte[] data2 = {5, 6, 7};

		// write and full read roundtrip
		policy.write("genmatch/blob", ReadData.from(data1));
		try (VolatileReadData result = policy.read("genmatch/blob")) {
			assertArrayEquals(data1, result.allBytes());
		}

		// after close(), a new read sees updated content
		policy.write("genmatch/blob", ReadData.from(data2));
		try (VolatileReadData result = policy.read("genmatch/blob")) {
			assertArrayEquals(data2, result.allBytes());
		}

		// concurrent modification: pin the generation via requireLength(),
		// overwrite the blob, then attempt to materialize
		policy.write("genmatch/blob", ReadData.from(data1));
		try (VolatileReadData vrd = policy.read("genmatch/blob")) {

			// sets the generation of vrd
			vrd.requireLength();

			// overwrite to new generation
			policy.write("genmatch/blob", ReadData.from(data2));

			// ensure fetching the data
			assertThrows(N5Exception.N5ConcurrentModificationException.class, vrd::allBytes);
		}
	}
}
