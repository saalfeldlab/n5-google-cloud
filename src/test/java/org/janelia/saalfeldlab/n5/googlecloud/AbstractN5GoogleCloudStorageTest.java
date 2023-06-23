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

import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.SecureRandom;

import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.junit.Assert;
import org.junit.Test;

import com.google.cloud.storage.Storage;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

/**
 * Base class for testing Google Cloud Storage N5 implementation.
 * Tests that are specific to Google Cloud can be added here.
 *
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
public abstract class AbstractN5GoogleCloudStorageTest extends AbstractN5Test {

	protected static Storage storage;

	public AbstractN5GoogleCloudStorageTest(final Storage storage) {

		AbstractN5GoogleCloudStorageTest.storage = storage;
	}

	private static final SecureRandom random = new SecureRandom();

	private static String generateName(String prefix, String suffix) {

		return prefix + Long.toUnsignedString(random.nextLong()) + suffix;
	}

	protected String tempBucketName() {

		return generateName("n5-test-", "-bucket");
	}

	protected String tempContainerPath() {

		return generateName("/n5-test-", ".n5");
	}

	@Override protected N5Writer createN5Writer() throws IOException, URISyntaxException {

		final URI uri = new URI(tempN5Location());
		final String bucketName = uri.getHost();
		final String basePath = uri.getPath();
		return new N5GoogleCloudStorageWriter(storage, bucketName, basePath, new GsonBuilder()) {

			@Override public void close() {

				remove();
				super.close();
			}
		};
	}

	@Override
	protected N5Writer createN5Writer(final String location, final GsonBuilder gson) throws IOException, URISyntaxException {

		final URI uri = new URI(location);
		final String bucketName = uri.getHost();
		final String basePath = uri.getPath();
		return new N5GoogleCloudStorageWriter(storage, bucketName, basePath, gson);
	}

	@Override
	protected N5Reader createN5Reader(final String location, final GsonBuilder gson) throws IOException, URISyntaxException {

		final URI uri = new URI(location);
		final String bucketName = uri.getHost();
		final String basePath = uri.getPath();
		return new N5GoogleCloudStorageReader(storage, bucketName, basePath, gson);
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
