/*-
 * #%L
 * N5 Google Cloud
 * %%
 * Copyright (C) 2017 - 2020 Igor Pisarev, Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.googlecloud;

import com.google.cloud.storage.Bucket;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.googlecloud.backend.BackendGoogleCloudStorageFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import com.google.cloud.storage.Storage;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

/**
 * Base class for testing Google Cloud Storage N5 implementation.
 * Tests that are specific to Google Cloud can be added here.
 *
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
@RunWith(Parameterized.class)
public class N5GoogleCloudStorageTest extends AbstractN5Test {

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data() {

		return Arrays.asList(new Object[][]{
				{"backend google cloud, container at generated path", null, false},
				{"backend google cloud, container at generated path , cache attributes", null, true},
				{"backend google cloud, container at root", "/", false},
				{"backend google cloud, container at root with , cache attributes", "/", true}
		});
	}

	protected static HashMap<Storage, ArrayList<String>> gsBuckets = new HashMap<>();
	private static final SecureRandom random = new SecureRandom();

	@Parameterized.Parameter(0)
	public String name;

	@Parameterized.Parameter(1)
	public String tempPath;

	@Parameterized.Parameter(2)
	public boolean useCache;

	private static String generateName(String prefix, String suffix) {

		return prefix + Long.toUnsignedString(random.nextLong()) + suffix;
	}

	protected static String tempBucketName(final Storage storage) {

		final String bucket = generateName("n5-test-", "-bucket");
		final ArrayList<String> gsResources = gsBuckets.getOrDefault(storage, new ArrayList<>());
		gsResources.add(bucket);
		gsBuckets.putIfAbsent(storage, gsResources);
		return bucket;
	}

	protected static String tempContainerPath() {

		return generateName("/n5-test-", ".n5");
	}

	@AfterClass
	public static void cleanup() {

		synchronized (gsBuckets) {
			for (Map.Entry<Storage, ArrayList<String>> gsBuckets : gsBuckets.entrySet()) {
				final Storage storage = gsBuckets.getKey();
				final ArrayList<String> buckets = gsBuckets.getValue();
				for (String bucket : buckets) {
					final Bucket asBucket = storage.get(bucket);
					if (asBucket != null && asBucket.exists())
						storage.delete(bucket);
				}
			}
			gsBuckets.clear();
		}
	}

	protected Storage getGoogleCloudStorage() {
		return BackendGoogleCloudStorageFactory.getOrCreateStorage();
	}

	@Override protected String tempN5Location() throws URISyntaxException, IOException {

		final String containerPath;
		if (tempPath != null)
			containerPath = tempPath;
		else
			containerPath = tempContainerPath();
		return new URI("gs", tempBucketName(getGoogleCloudStorage()), containerPath, null).toString();
	}

	@Override protected N5Writer createN5Writer() throws IOException, URISyntaxException {

		final URI uri = new URI(tempN5Location());
		final String bucketName = uri.getHost();
		final String basePath = uri.getPath();
		return new N5GoogleCloudStorageWriter(getGoogleCloudStorage(), bucketName, basePath, new GsonBuilder()) {

			@Override public void close() {

				remove();
				super.close();
			}
		};
	}

	@Override
	protected N5Writer createN5Writer(final String location, final GsonBuilder gson) throws URISyntaxException {

		final URI uri = new URI(location);
		final String bucketName = uri.getHost();
		final String basePath = uri.getPath();
		return new N5GoogleCloudStorageWriter(getGoogleCloudStorage(), bucketName, basePath, gson);
	}

	@Override
	protected N5Reader createN5Reader(final String location, final GsonBuilder gson) throws IOException, URISyntaxException {

		final URI uri = new URI(location);
		final String bucketName = uri.getHost();
		final String basePath = uri.getPath();
		return new N5GoogleCloudStorageReader(getGoogleCloudStorage(), bucketName, basePath, gson);
	}

	/**
	 * Currently, {@code N5GoogleCloudStorageReader#exists(String)} is implemented by listing objects under that group.
	 * This test case specifically tests its correctness.
	 *
	 * @throws IOException
	 */
	@Test
	public void testExistsUsingListingObjects() throws IOException, URISyntaxException {

		try (N5Writer n5 = createN5Writer()) {
			n5.createGroup("/one/two/three");

			Assert.assertTrue(n5.exists(""));
			Assert.assertTrue(n5.exists("/"));

			Assert.assertTrue(n5.exists("one"));
			Assert.assertTrue(n5.exists("one/"));
			Assert.assertTrue(n5.exists("/one"));
			Assert.assertTrue(n5.exists("/one/"));

			Assert.assertTrue(n5.exists("one/two"));
			Assert.assertTrue(n5.exists("one/two/"));
			Assert.assertTrue(n5.exists("/one/two"));
			Assert.assertTrue(n5.exists("/one/two/"));

			Assert.assertTrue(n5.exists("one/two/three"));
			Assert.assertTrue(n5.exists("one/two/three/"));
			Assert.assertTrue(n5.exists("/one/two/three"));
			Assert.assertTrue(n5.exists("/one/two/three/"));

			Assert.assertFalse(n5.exists("one/tw"));
			Assert.assertFalse(n5.exists("one/tw/"));
			Assert.assertFalse(n5.exists("/one/tw"));
			Assert.assertFalse(n5.exists("/one/tw/"));

			Assert.assertArrayEquals(new String[]{"one"}, n5.list("/"));
			Assert.assertArrayEquals(new String[]{"two"}, n5.list("/one"));
			Assert.assertArrayEquals(new String[]{"three"}, n5.list("/one/two"));

			Assert.assertArrayEquals(new String[]{}, n5.list("/one/two/three"));
			assertThrows(N5Exception.N5IOException.class, () -> n5.list("/one/tw"));

			Assert.assertTrue(n5.remove("/one/two/three"));
			Assert.assertFalse(n5.exists("/one/two/three"));
			Assert.assertTrue(n5.exists("/one/two"));
			Assert.assertTrue(n5.exists("/one"));

			Assert.assertTrue(n5.remove("/one"));
			Assert.assertFalse(n5.exists("/one/two"));
			Assert.assertFalse(n5.exists("/one"));
		}
	}

	@Override
	@Test public void testReaderCreation() throws IOException, URISyntaxException {

		try (N5Writer writer = createN5Writer()) {
			final String canonicalPath = writer.getURI().toString();


			final N5Reader n5r = createN5Reader(canonicalPath);
			assertNotNull(n5r);

			// existing directory without attributes is okay;
			// Remove and create to remove attributes store
			writer.removeAttribute("/", "/");
			final N5Reader na = createN5Reader(canonicalPath);
			assertNotNull(na);

			// existing location with attributes, but no version
			writer.removeAttribute("/", "/");
			writer.setAttribute("/", "mystring", "ms");
			final N5Reader wa = createN5Reader(canonicalPath);
			assertNotNull(wa);

			// existing directory with incompatible version should fail
			writer.removeAttribute("/", "/");
			writer.setAttribute("/", N5Reader.VERSION_KEY,
					new N5Reader.Version(N5Reader.VERSION.getMajor() + 1, N5Reader.VERSION.getMinor(), N5Reader.VERSION.getPatch()).toString());
			assertThrows("Incompatible version throws error", N5Exception.N5IOException.class,
					() -> createN5Reader(canonicalPath));
			writer.remove();
		}
		/* In the AbstractN5Test class, there is a final test to ensure the reader creation fails if the container doesn't exist.
		* Unfortunately, the google cloud storage test framework doesn't support that during testing,
		* so we cannot support it. If future cloud store testing frameworks support creating mock buckets, we can test then. */
	}
}
