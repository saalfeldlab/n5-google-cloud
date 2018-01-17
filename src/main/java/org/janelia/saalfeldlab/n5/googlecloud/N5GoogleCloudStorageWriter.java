/**
 * Copyright (c) 2017, Stephan Saalfeld
 * All rights reserved.
 *
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
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5.googlecloud;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DefaultBlockWriter;
import org.janelia.saalfeldlab.n5.GsonAttributesParser;
import org.janelia.saalfeldlab.n5.N5Writer;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageException;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

/**
 * N5 implementation using Google Cloud Storage backend with version compatibility check.
 *
 * This implementation enforces that an empty attributes file is present for each group.
 * It is used for determining group existence and listing groups.
 *
 * @author Igor Pisarev
 */
public class N5GoogleCloudStorageWriter extends N5GoogleCloudStorageReader implements N5Writer {

	/**
	 * Opens an {@link N5GoogleCloudStorageReader} using a {@link Storage} client and a given bucket name
	 * with a custom {@link GsonBuilder} to support custom attributes.
	 *
	 * @param storage
	 * @param bucketName
	 * @param gsonBuilder
	 * @throws IOException
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final String bucketName, final GsonBuilder gsonBuilder) throws IOException {

		super(storage, bucketName, gsonBuilder);

		// bucket creation is not supported in the mock library: https://github.com/GoogleCloudPlatform/google-cloud-java/issues/2106
		if (storage.get(bucketName) == null)
			handleUnsupportedOperationException(() -> storage.create(BucketInfo.of(bucketName)));

		setAttribute("/", VERSION_KEY, VERSION.toString());
	}

	/**
	 * Opens an {@link N5GoogleCloudStorageReader} using a {@link Storage} client and a given bucket name.
	 *
	 * @param storage
	 * @param bucketName
	 * @param gsonBuilder
	 * @throws IOException
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final String bucketName) throws IOException {

		this(storage, bucketName, new GsonBuilder());
	}

	@Override
	public void createGroup(final String pathName) throws IOException {

		final Path path = Paths.get(pathName);
		for (int i = 0; i < path.getNameCount(); ++i) {
			final String subgroup = path.subpath(0, i + 1).toString();
			if (!exists(subgroup))
				setAttributes(subgroup, Collections.emptyMap());
		}
	}

	@Override
	public void setAttributes(
			final String pathName,
			final Map<String, ?> attributes) throws IOException {

		final HashMap<String, JsonElement> map = getAttributes(pathName);
		GsonAttributesParser.insertAttributes(map, attributes, gson);

		try (final ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
			GsonAttributesParser.writeAttributes(new OutputStreamWriter(byteStream), map, gson);
			writeBlob(getAttributesKey(pathName), byteStream.toByteArray());
		}
	}

	@Override
	public <T> void writeBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws IOException {

		try (final ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
			DefaultBlockWriter.writeBlock(byteStream, datasetAttributes, dataBlock);
			writeBlob(getDataBlockKey(pathName, dataBlock.getGridPosition()), byteStream.toByteArray());
		}
	}

	@Override
	public boolean remove() throws IOException {

		// the mock library always returns false for buckets, account for that when returning the final status
		final boolean bucketWasFound = storage.get(bucketName) != null;
		final boolean blobsRemovalStatus = remove("");
		final boolean bucketRemovalStatus = storage.delete(bucketName);
		return blobsRemovalStatus && (bucketWasFound ? bucketRemovalStatus : true);
	}

	@Override
	public boolean remove(final String pathName) throws IOException {

		final String correctedPathName = removeFrontDelimiter(ensureCorrectDelimiter(pathName));
		final String prefix = correctedPathName.isEmpty() ? "" : appendDelimiter(correctedPathName);

		final List<BlobId> subBlobs = new ArrayList<>();
		final Page<Blob> blobListing = storage.list(bucketName, BlobListOption.prefix(prefix));
		for (final Iterator<Blob> blobIterator = blobListing.iterateAll().iterator(); blobIterator.hasNext();) {
			final Blob nextBlob = blobIterator.next();
			subBlobs.add(nextBlob.getBlobId());
		}

		handleUnsupportedOperationException(
				() -> storage.delete(subBlobs),
				() -> {
					for (final BlobId blobId : subBlobs)
						storage.delete(blobId);
				}
			);
		return !exists(pathName);
	}

	protected void writeBlob(
			final String blobKey,
			final byte[] bytes) throws IOException {

		final BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, blobKey).build();
		storage.create(blobInfo, bytes);
	}

	protected void handleUnsupportedOperationException(final Runnable runnable) {

		handleUnsupportedOperationException(runnable, null);
	}

	protected void handleUnsupportedOperationException(final Runnable runnable, final Runnable fallback) {

		try {
			runnable.run();
		} catch (final StorageException | UnsupportedOperationException e) {
			if (e instanceof UnsupportedOperationException || e.getCause() instanceof UnsupportedOperationException) {
				// operation not supported (possibly the mock library is being used)
				if (fallback != null)
					fallback.run();
			} else {
				throw e;
			}
		}
	}
}
