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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.janelia.saalfeldlab.googlecloud.GoogleCloudStorageURI;
import org.janelia.saalfeldlab.n5.AbstractGsonReader;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DefaultBlockReader;
import org.janelia.saalfeldlab.n5.GsonAttributesParser;
import org.janelia.saalfeldlab.n5.N5Reader;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobField;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.spi.v1.HttpStorageRpc;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

/**
 * N5 implementation using Google Cloud Storage backend with version compatibility check.
 *
 * @author Igor Pisarev
 */
public class N5GoogleCloudStorageReader extends AbstractGsonReader implements N5Reader {

	protected static final String jsonFile = "attributes.json";

	protected final Storage storage;
	protected final String bucketName;
	protected final String containerPath;

	/**
	 * Opens an {@link N5GoogleCloudStorageReader} using a {@link Storage} client and a given bucket name.
	 *
	 * If the bucket does not exist, it will not be created and
	 * all subsequent attempts to read attributes, groups, or datasets will fail.
	 *
	 * @param storage
	 * @param bucketName
	 * @throws IOException
	 */
	public N5GoogleCloudStorageReader(final Storage storage, final String bucketName) throws IOException {

		this(storage, bucketName, new GsonBuilder());
	}

	/**
	 * Opens an {@link N5GoogleCloudStorageReader} using a {@link Storage} client, a given bucket name,
	 * and a path to the container within the bucket.
	 *
	 * If the bucket and/or container does not exist, it will not be created and
	 * all subsequent attempts to read attributes, groups, or datasets will fail.
	 *
	 * @param storage
	 * @param bucketName
	 * @param containerPath
	 * @throws IOException
	 */
	public N5GoogleCloudStorageReader(final Storage storage, final String bucketName, final String containerPath) throws IOException {

		this(storage, bucketName, containerPath, new GsonBuilder());
	}

	/**
	 * Opens an {@link N5GoogleCloudStorageReader} using a {@link Storage} client and a given Google Cloud Storage URI.
	 *
	 * If the bucket and/or container does not exist, it will not be created and
	 * all subsequent attempts to read attributes, groups, or datasets will fail.
	 *
	 * @param storage
	 * @param containerURI
	 * @throws IOException
	 */
	public N5GoogleCloudStorageReader(final Storage storage, final GoogleCloudStorageURI containerURI) throws IOException {

		this(storage, containerURI, new GsonBuilder());
	}

	/**
	 * Opens an {@link N5GoogleCloudStorageReader} using a {@link Storage} client and a given Google Cloud Storage URI
	 * with a custom {@link GsonBuilder} to support custom attributes.
	 *
	 * If the bucket and/or container does not exist, it will not be created and
	 * all subsequent attempts to read attributes, groups, or datasets will fail.
	 *
	 * @param storage
	 * @param containerURI
	 * @param gsonBuilder
	 * @throws IOException
	 */
	public N5GoogleCloudStorageReader(final Storage storage, final GoogleCloudStorageURI containerURI, final GsonBuilder gsonBuilder) throws IOException {

		this(storage, containerURI.getBucket(), containerURI.getKey(), gsonBuilder);
	}

	/**
	 * Opens an {@link N5GoogleCloudStorageReader} using a {@link Storage} client and a given bucket name
	 * with a custom {@link GsonBuilder} to support custom attributes.
	 *
	 * If the bucket does not exist, it will not be created and
	 * all subsequent attempts to read attributes, groups, or datasets will fail.
	 *
	 * @param storage
	 * @param bucketName
	 * @param gsonBuilder
	 * @throws IOException
	 */
	public N5GoogleCloudStorageReader(final Storage storage, final String bucketName, final GsonBuilder gsonBuilder) throws IOException {

		this(storage, bucketName, "/", gsonBuilder);
	}

	/**
	 * Opens an {@link N5GoogleCloudStorageReader} using a {@link Storage} client, a given bucket name,
	 * and a path to the container within the bucket with a custom {@link GsonBuilder} to support custom attributes.
	 *
	 * If the bucket and/or container does not exist, it will not be created and
	 * all subsequent attempts to read attributes, groups, or datasets will fail.
	 *
	 * @param storage
	 * @param bucketName
	 * @param containerPath
	 * @param gsonBuilder
	 * @throws IOException
	 */
	public N5GoogleCloudStorageReader(
			final Storage storage,
			final String bucketName,
			final String containerPath,
			final GsonBuilder gsonBuilder) throws IOException {

		super(gsonBuilder);

		this.storage = storage;
		this.bucketName = bucketName;
		this.containerPath = containerPath;

		if (storage.get(bucketName) != null) {
			final Version version = getVersion();
			if (!VERSION.isCompatible(version))
				throw new IOException("Incompatible version " + version + " (this is " + VERSION + ").");
		}
	}

	@Override
	public boolean exists(final String pathName) {

		final String fullPath = getFullPath(pathName);
		final String prefix = fullPath.isEmpty() ? "" : addTrailingSlash(fullPath);
		final BlobListOption[] blobListOptions = allOperationsSupported() ?
				new BlobListOption[] { BlobListOption.prefix(prefix), BlobListOption.pageSize(1) } :
					new BlobListOption[] { BlobListOption.prefix(prefix) };
		final Page<Blob> blobListing = storage.list(bucketName, blobListOptions);
		return blobListing.getValues().iterator().hasNext();
	}

	@Override
	public HashMap<String, JsonElement> getAttributes(final String pathName) throws IOException {

		final String attributesKey = getAttributesKey(pathName);
		final Blob attributesBlob = getBlob(attributesKey);
		if (!blobExists(attributesBlob))
			return new HashMap<>();

		try (final InputStream in = readBlob(attributesBlob)) {
			return GsonAttributesParser.readAttributes(new InputStreamReader(in), gson);
		}
	}

	@Override
	public DataBlock<?> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long[] gridPosition) throws IOException {

		final String dataBlockKey = getDataBlockKey(pathName, gridPosition);
		final Blob dataBlockBlob = getBlob(dataBlockKey);
		if (!blobExists(dataBlockBlob))
			return null;

		try (final InputStream in = readBlob(dataBlockBlob)) {
			return DefaultBlockReader.readBlock(in, datasetAttributes, gridPosition);
		}
	}

	@Override
	public String[] list(final String pathName) throws IOException {

		final String fullPath = getFullPath(pathName);
		final String prefix = fullPath.isEmpty() ? "" : addTrailingSlash(fullPath);
		final Path path = Paths.get(prefix);

		final List<String> subGroups = new ArrayList<>();
		final Page<Blob> blobListing = storage.list(
				bucketName,
				BlobListOption.prefix(prefix),
				BlobListOption.currentDirectory(),
				BlobListOption.fields(BlobField.ID));
		for (final Iterator<Blob> blobIterator = blobListing.iterateAll().iterator(); blobIterator.hasNext();) {
			final Blob nextBlob = blobIterator.next();
			final String blobName = nextBlob.getBlobId().getName();
			if (blobName.endsWith("/")) {
				final Path relativePath = path.relativize(Paths.get(blobName));
				final String correctedSubgroupPathName = replaceBackSlashes(relativePath.toString());
				if (!correctedSubgroupPathName.isEmpty())
					subGroups.add(correctedSubgroupPathName);
			}
		}
		return subGroups.toArray(new String[subGroups.size()]);
	}

	protected Blob getBlob(final String blobKey) {

		return storage.get(BlobId.of(bucketName, blobKey));
	}

	protected InputStream readBlob(final Blob blob) {

		final byte[] bytes = blob.getContent();
		return new ByteArrayInputStream(bytes);
	}

	protected boolean blobExists(final Blob blob) {

		return blob != null && blob.exists();
	}

	protected boolean allOperationsSupported() {

		// some operations are not supported in the mock library, need to check whether a real or a fake client is being used
		// would be better to check against FakeStorageRpc, but it is not desired to include the mock library as a non-test dependency
		return storage.getOptions().getRpc() instanceof HttpStorageRpc;
	}

	/**
	 * Google Cloud service accepts only forward slashes as path delimiters.
	 * This method replaces back slashes to forward slashes (if any) and returns a corrected path name.
	 *
	 * @param pathName
	 * @return
	 */
	protected static String replaceBackSlashes(final String pathName) {

		return pathName.replace("\\", "/");
	}

	/**
	 * When absolute paths are passed (e.g. /group/data), Google Cloud service creates an additional root folder with an empty name.
	 * This method removes the root slash symbol and returns the corrected path.
	 *
	 * Additionally, it ensures correctness on both Unix and Windows platforms, otherwise {@code pathName} is treated
	 * as UNC path on Windows, and {@code Paths.get(pathName, ...)} fails with {@code InvalidPathException}.
	 *
	 * @param pathName
	 * @return
	 */
	protected static String removeLeadingSlash(final String pathName) {

		return pathName.startsWith("/") || pathName.startsWith("\\") ? pathName.substring(1) : pathName;
	}

	/**
	 * When listing children objects for a group, must append a delimiter to the path (e.g. group/data/).
	 * This is necessary for not including wrong objects in the filtered set
	 * (e.g. group/data-2/attributes.json when group/data is passed without the last slash).
	 *
	 * @param pathName
	 * @return
	 */
	protected static String addTrailingSlash(final String pathName) {

		return pathName.endsWith("/") || pathName.endsWith("\\") ? pathName : pathName + "/";
	}

	/**
	 * Constructs the path for a data block in a dataset at a given grid position.
	 *
	 * The returned path is
	 * <pre>
	 * $datasetPathName/$gridPosition[0]/$gridPosition[1]/.../$gridPosition[n]
	 * </pre>
	 *
	 * This is the file into which the data block will be stored.
	 *
	 * @param datasetPathName
	 * @param gridPosition
	 * @return
	 */
	protected String getDataBlockKey(
			final String datasetPathName,
			final long[] gridPosition) {

		final String[] pathComponents = new String[gridPosition.length];
		for (int i = 0; i < pathComponents.length; ++i)
			pathComponents[i] = Long.toString(gridPosition[i]);

		final String dataBlockPathName = Paths.get(removeLeadingSlash(datasetPathName), pathComponents).toString();
		return getFullPath(dataBlockPathName);
	}

	/**
	 * Constructs a full path for a path that is relative to the container.
	 *
	 * @param relativePath
	 * @return
	 */
	protected String getFullPath(final String relativePath) {

		final String fullPath = Paths.get(removeLeadingSlash(containerPath), relativePath).toString();
		return removeLeadingSlash(replaceBackSlashes(fullPath));
	}

	/**
	 * Constructs the path for the attributes file of a group or dataset.
	 *
	 * @param pathName
	 * @return
	 */
	protected String getAttributesKey(final String pathName) {

		final String attributesPath = Paths.get(removeLeadingSlash(pathName), jsonFile).toString();
		return getFullPath(attributesPath);
	}

	/**
	 * Determines whether the current N5 container is stored at the root level of the bucket.
	 *
	 * @return
	 */
	protected boolean isContainerBucketRoot() {
		return removeLeadingSlash(containerPath).isEmpty();
	}
}