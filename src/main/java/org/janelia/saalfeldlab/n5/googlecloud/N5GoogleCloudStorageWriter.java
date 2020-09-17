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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.janelia.saalfeldlab.googlecloud.GoogleCloudStorageURI;
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
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

/**
 * N5 implementation using Google Cloud Storage backend with version compatibility check.
 *
 * @author Igor Pisarev
 */
public class N5GoogleCloudStorageWriter extends N5GoogleCloudStorageReader implements N5Writer {

	/**
	 * Opens an {@link N5GoogleCloudStorageWriter} using a {@link Storage} client and a given bucket name.
	 *
	 * @param storage
	 * @param bucketName
	 * @throws IOException
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final String bucketName) throws IOException {

		this(storage, bucketName, new GsonBuilder());
	}

	/**
	 * Opens an {@link N5GoogleCloudStorageWriter} using a {@link Storage} client, a given bucket name,
	 * and a path to the container within the bucket.
	 *
	 * @param storage
	 * @param bucketName
	 * @param containerPath
	 * @throws IOException
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final String bucketName, final String containerPath) throws IOException {

		this(storage, bucketName, containerPath, new GsonBuilder());
	}

	/**
	 * Opens an {@link N5GoogleCloudStorageWriter} using a {@link Storage} client and a given Google Cloud Storage URI.
	 *
	 * @param storage
	 * @param containerURI
	 * @throws IOException
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final GoogleCloudStorageURI containerURI) throws IOException {

		this(storage, containerURI, new GsonBuilder());
	}

	/**
	 * Opens an {@link N5GoogleCloudStorageWriter} using a {@link Storage} client and a given Google Cloud Storage URI
	 * with a custom {@link GsonBuilder} to support custom attributes.
	 *
	 * @param storage
	 * @param containerURI
	 * @param gsonBuilder
	 * @throws IOException
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final GoogleCloudStorageURI containerURI, final GsonBuilder gsonBuilder) throws IOException {

		this(storage, containerURI.getBucket(), containerURI.getKey(), gsonBuilder);
	}

	/**
	 * Opens an {@link N5GoogleCloudStorageWriter} using an{@link Storage} client and a given bucket name
	 * with a custom {@link GsonBuilder} to support custom attributes.
	 *
	 * @param storage
	 * @param bucketName
	 * @param gsonBuilder
	 * @throws IOException
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final String bucketName, final GsonBuilder gsonBuilder) throws IOException {

		this(storage, bucketName, "/", gsonBuilder);
	}

	/**
	 * Opens an {@link N5GoogleCloudStorageWriter} using a {@link Storage} client, a given bucket name,
	 * and a path to the container within the bucket with a custom {@link GsonBuilder} to support custom attributes.
	 *
	 * @param storage
	 * @param bucketName
	 * @param containerPath
	 * @param gsonBuilder
	 * @throws IOException
	 */
	public N5GoogleCloudStorageWriter(
			final Storage storage,
			final String bucketName,
			final String containerPath,
			final GsonBuilder gsonBuilder) throws IOException {

		super(storage, bucketName, containerPath, gsonBuilder);

		if (allOperationsSupported() && storage.get(bucketName) == null)
			storage.create(BucketInfo.of(bucketName));

		if (!isContainerBucketRoot() && !exists("/"))
			createGroup("/");

		if (!VERSION.equals(getVersion()))
			setAttribute("/", VERSION_KEY, VERSION.toString());
	}

	@Override
	public void createGroup(final String pathName) throws IOException {

		final Path groupPath = Paths.get(removeLeadingSlash(pathName));
		for (int i = 0; i < groupPath.getNameCount(); ++i) {
			final String parentGroupPath = groupPath.subpath(0, i + 1).toString();
			final String fullParentGroupPath = getFullPath(parentGroupPath);
			writeBlob(replaceBackSlashes(addTrailingSlash(removeLeadingSlash(fullParentGroupPath))), null);
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
	public boolean deleteBlock(final String pathName, final long[] gridPosition) throws IOException {

		final String dataBlockKey = getDataBlockKey(pathName, gridPosition);
		final Blob dataBlockBlob = getBlob(dataBlockKey);
		if (blobExists(dataBlockBlob))
			dataBlockBlob.delete();
		return !blobExists(dataBlockBlob);
	}

	@Override
	public boolean remove() throws IOException {

		// the mock library always returns false for buckets, account for that when returning the final status
		final boolean wasBucketFound = storage.get(bucketName) != null;
		final boolean wasPathRemoved = remove("/");

		if (!isContainerBucketRoot() || !wasPathRemoved)
			return wasPathRemoved;

		// N5 container was at the root level of the bucket so the bucket needs to be removed as well
		final boolean wasBucketRemoved = storage.delete(bucketName);
		return wasBucketFound ? wasBucketRemoved : true;
	}

	@Override
	public boolean remove(final String pathName) throws IOException {

		final String fullPath = getFullPath(pathName);
		final String prefix = fullPath.isEmpty() ? "" : addTrailingSlash(fullPath);

		final List<BlobId> subBlobs = new ArrayList<>();
		final Page<Blob> blobListing = storage.list(bucketName, BlobListOption.prefix(prefix));
		for (final Iterator<Blob> blobIterator = blobListing.iterateAll().iterator(); blobIterator.hasNext();) {
			final Blob nextBlob = blobIterator.next();
			subBlobs.add(nextBlob.getBlobId());
		}

		if (allOperationsSupported()) {
			storage.delete(subBlobs);
		} else {
			for (final BlobId blobId : subBlobs)
				storage.delete(blobId);
		}

		return !exists(pathName);
	}

	protected void writeBlob(
			final String blobKey,
			final byte[] bytes) throws IOException {

		final BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, blobKey).build();
		storage.create(blobInfo, bytes);
	}
}
