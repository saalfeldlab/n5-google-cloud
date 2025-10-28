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

import com.google.cloud.storage.Storage;
import com.google.gson.GsonBuilder;
import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5KeyValueReader;
import org.janelia.saalfeldlab.n5.N5KeyValueWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.googlecloud.backend.BackendGoogleCloudStorageFactory;
import org.junit.AfterClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;

/**
 * Base class for testing Google Cloud Storage N5 implementation.
 * Tests that are specific to Google Cloud can be added here.
 *
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
@RunWith(Parameterized.class)
public class N5GoogleCloudStorageTests extends AbstractN5Test {



	public enum LocationInBucket {
		ROOT(() -> "/", N5GoogleCloudStorageTests::tempBucketName),
		KEY(N5GoogleCloudStorageTests::tempContainerPath, tempBucketName()::toString);

		public final Supplier<String> getContainerPath;
		private final Supplier<String> getBucketName;
		LocationInBucket(Supplier<String> tempContainerPath, Supplier<String> tempBucketaName) {

			this.getContainerPath = tempContainerPath;
			this.getBucketName = tempBucketaName;
		}

		String getPath() {

			return getContainerPath.get();
		}

		String getBucketName() {

			return getBucketName.get();
		}
	}

	public enum UseCache {
		CACHE(true),
		NO_CACHE(false);

		final boolean cache;

		UseCache(boolean cache) {

			this.cache = cache;
		}
	}

	@Parameterized.Parameters(name = "Container at {0}, {1}")
	public static Collection<Object[]> data() {

		return Arrays.asList(new Object[][]{
				{LocationInBucket.ROOT, UseCache.NO_CACHE},
				{LocationInBucket.ROOT, UseCache.CACHE},
				{LocationInBucket.KEY, UseCache.NO_CACHE},
				{LocationInBucket.KEY, UseCache.CACHE}
		});
	}

	private static final SecureRandom random = new SecureRandom();

	@Parameterized.Parameter()
	public LocationInBucket containerLocation;

	@Parameterized.Parameter(1)
	public UseCache useCache;

	private static Storage lateinitStorage;
	{
		lateinitStorage = getGoogleCloudStorage();
	}

	@Parameterized.AfterParam
	public static void removeTestBucket(LocationInBucket containerLocation, UseCache ignore) {
		if (containerLocation != LocationInBucket.ROOT)
			return;

		final String bucketName = containerLocation.getBucketName();
		try {
			final GoogleCloudStorageKeyValueAccess kva = new GoogleCloudStorageKeyValueAccess(lateinitStorage, N5URI.encodeAsUri("gs://" + bucketName), true);
			kva.delete(kva.normalize("/"));
		} catch (Exception e) {
			System.err.println("Exception After Tests, Could Not Delete Test Bucket:" + bucketName);
			e.printStackTrace();
		}
	}

	@AfterClass
	public static void afterClass() {
		final String bucketName = LocationInBucket.KEY.getBucketName();
		try {
			final GoogleCloudStorageKeyValueAccess kva = new GoogleCloudStorageKeyValueAccess(lateinitStorage, N5URI.encodeAsUri("gs://" + bucketName), true);
			kva.delete(kva.normalize("/"));
		} catch (Exception e) {
			System.err.println("Exception After Tests, Could Not Delete Test Bucket:" + bucketName);
			e.printStackTrace();
		}
	}

	private static String generateName(String prefix, String suffix) {

		return prefix + Long.toUnsignedString(random.nextLong()) + suffix;
	}

	public static String tempBucketName() {

		return generateName("n5-test-", "-bucket");
	}

	protected static String tempContainerPath() {

		return generateName("/n5-test-", ".n5");
	}

	protected Storage getGoogleCloudStorage() {

		return BackendGoogleCloudStorageFactory.getOrCreateStorage();
	}

	@Override public void testAttributePathEscaping() {

		super.testAttributePathEscaping();
	}

	@Override protected String tempN5Location() throws URISyntaxException {

		final String containerPath = containerLocation.getPath();
		final String testBucket = containerLocation.getBucketName();
		return new URI("gs", testBucket, containerPath, null).toString();
	}

	@Override protected N5Writer createN5Writer() throws URISyntaxException, IOException {

		final String containerUri = tempN5Location();
		return createN5Writer(containerUri);
	}

	@Override
	protected N5Writer createN5Writer(final String location, final GsonBuilder gson) throws URISyntaxException {

		final Storage storage = getGoogleCloudStorage();
		final String uriString = location.startsWith("gs://") ? location : "gs://" + location;
		final KeyValueAccess kva = new GoogleCloudStorageKeyValueAccess(storage, N5URI.encodeAsUri(uriString), true);
		return new N5KeyValueWriter(kva, uriString, gson, useCache.cache);
	}

	@Override
	protected N5Reader createN5Reader(final String location, final GsonBuilder gson) throws IOException, URISyntaxException {

		final Storage storage = getGoogleCloudStorage();
		final KeyValueAccess kva = new GoogleCloudStorageKeyValueAccess(storage, N5URI.encodeAsUri(location), false);
		return new N5KeyValueReader(kva, location, gson, useCache.cache);
	}
}
