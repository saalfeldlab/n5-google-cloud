package org.janelia.saalfeldlab.n5.googlecloud;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobField;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Objects;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudStorageURI;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.KeyValueRoot;
import org.janelia.saalfeldlab.n5.LockingPolicy;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5ConcurrentModificationException;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.N5Path;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.janelia.saalfeldlab.n5.N5Path.N5FilePath;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.readdata.LazyRead;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

import static org.janelia.saalfeldlab.n5.LockingPolicy.UNSAFE;

public class GoogleCloudStorageKeyValueRoot implements KeyValueRoot {

	/*
	 * Error codes
	 */
	private final static int NOT_FOUND = 404;

	// TODO: URI this KVR was constructed with.
	//       Shouldn't be used internally.
	//       Maybe it will be removed later in favour of canonical URI
	private final GoogleCloudStorageURI containerURI;

	private final Storage storage;
	private final String bucket;
	private final N5DirectoryPath root;
	private final boolean generationMatch;

	private final boolean createBucket;

	private Boolean bucketCheckedAndExists = null;

	/**
	 * Creates a {@link GoogleCloudStorageKeyValueRoot} using a google cloud
	 * storage backend.
	 *
	 * @param storage
	 * 		the google cloud interface
	 * @param bucketURI
	 * 		the URI of the bucket
	 * @param basePath
	 * 		the n5 container root relative to the bucket
	 * @param createBucket
	 * 		whether the bucket should be created if it doesn't exist
	 * @throws N5IOException
	 * 		if the access could not be created
	 */
	public GoogleCloudStorageKeyValueRoot(
			final Storage storage,
			final String bucketURI,
			final String basePath,
			final boolean createBucket) throws N5IOException {

		this(storage, toContainerURI(bucketURI, basePath), createBucket);
	}

	/**
	 * Creates a {@link GoogleCloudStorageKeyValueRoot} using a google cloud
	 * storage backend.
	 *
	 * @param storage
	 * 		the google cloud interface
	 * @param containerURI
	 * 		the URI of the n5 container root
	 * @param createBucket
	 * 		whether the bucket should be created if it doesn't exist
	 *
	 * @throws N5IOException
	 * 		if the access could not be created
	 */
	public GoogleCloudStorageKeyValueRoot(
			final Storage storage,
			final GoogleCloudStorageURI containerURI,
			final boolean createBucket) throws N5IOException {

		this.containerURI = containerURI;

		this.storage = storage;
		this.bucket = containerURI.getBucket();
		this.root = N5DirectoryPath.of(containerURI.getKey());
		this.createBucket = createBucket;

		final LockingPolicy policy = LockingPolicy.fromString(System.getProperty("n5.ioPolicy", "strict"));
		generationMatch = (policy != UNSAFE);
	}

	private static GoogleCloudStorageURI toContainerURI(final String bucketURI, final String basePath)
			throws N5Exception {
		try {
			return new GoogleCloudStorageURI(N5URI.getAsUri(bucketURI).resolve(basePath));
		} catch (final Exception e) {
			throw new N5Exception("Could not construct GoogleCloudStorageURI from bucketURI=\"" + bucketURI + "\", basePath=\"" + basePath + "\"", e);
		}
	}

	// ------------------------------------------------------------------------
	//
	// -- from GoogleCloudStorageKeyValueAccess --
	//

	private boolean bucketExists() {

		if (bucketCheckedAndExists == null) {
			final Bucket b = storage.get(bucket);
			bucketCheckedAndExists = b != null && b.exists();
		}
		return bucketCheckedAndExists;
	}

	private void createBucket() {

		if (!createBucket)
			throw new N5Exception("Create Bucket Not Allowed");

		if (bucketExists())
			return;

		try {
			storage.create(BucketInfo.of(bucket));
			bucketCheckedAndExists = true;
		} catch (Exception e) {
			throw new N5IOException("Could not create bucket " + bucket, e);
		}
	}

	private void deleteBucket() {

		if (!createBucket)
			throw new N5Exception("Delete Bucket Not Allowed");

		// Not pointless, flag is Boolean, not boolean, and could be `null`
		if (Objects.equal(bucketCheckedAndExists, false))
			return;

		storage.delete(bucket);
		bucketCheckedAndExists = false;
	}

	private boolean prefixExists(final String key) throws N5IOException {

		// not every directory will have an empty "directory" key stored in the backend,
		// for example, if the container contents was copied to GCS with the cli
		// in that case, check if any keys exist with the prefix, if so, it's a directory
		try {
			return storage.list(bucket,
							BlobListOption.prefix(key),
							BlobListOption.pageSize(1),
							BlobListOption.currentDirectory())
					.iterateAll().iterator().hasNext();
		} catch (final StorageException e) {
			if (e.getCode() == NOT_FOUND)
				return false;
			throw new N5IOException("Could not get key " + key, e);
		}
	}

	/**
	 * Check existence of the given {@code key}.
	 *
	 * @param key the object key
	 * @return {@code true} if {@code key} exists.
	 */
	private boolean keyExists(final String key) throws N5IOException {

		try {
			final Blob blob = storage.get(BlobId.of(bucket, key), BlobGetOption.fields());
			return blob != null && blob.exists();
		} catch (final StorageException e) {
			throw new N5IOException("Could not get key " + key, e);
		}

	}


	//
	// -- from GoogleCloudStorageKeyValueAccess --
	//
	// ------------------------------------------------------------------------

	@Override
	public synchronized KeyValueAccess getKVA() {
		if (kva == null) {
			kva = new GoogleCloudStorageKeyValueAccess(storage, containerURI, createBucket);
		}
		return kva;
	}
	private GoogleCloudStorageKeyValueAccess kva;


	@Override
	public URI uri() {
		return containerURI.asURI();
		// TODO: return canonical ("gs:/") URI instead
	}

	@Override
	public VolatileReadData createReadData(final N5FilePath normalPath) throws N5IOException {

		final String key = root.resolve(normalPath).path();
		return VolatileReadData.from(new GCSLazyRead(key));
	}

	/**
	 * Test whether the path is a directory.
	 * <p>
	 * Appends trailing "/" to {@code normalPath} if there is none and then
	 * checks whether resulting {@code path} is a key.
	 *
	 * @param normalPath
	 * 		(relative to container root)
	 * 		is expected to be in normalized form, no further efforts are made to normalize it.
	 * @return {@code true} if {@code path} (with trailing "/") exists as a key, {@code false} otherwise
	 */
	@Override
	public boolean isDirectory(final N5Path normalPath) throws N5IOException {

		final String prefix = root.resolve(normalPath.asDirectory()).path();

		if (!bucketExists())
			return false;

		if (prefix.isEmpty() || prefixExists(prefix))
			return true;

		try {
			// may be no children, but may have proper `directory` key
			final Blob blob = storage.get(bucket, prefix, BlobGetOption.fields(BlobField.SIZE));
			if (blob != null)
				return blob.getSize() == 0;
		} catch (final StorageException ignore) {
		}

		return false;
	}

	@Override
	public boolean isFile(final N5Path normalPath) throws N5IOException {

		if (normalPath.isDirectory()) {
			return false;
		}

		final String key = root.resolve(normalPath).path();
		return bucketExists() && keyExists(key);
	}

	@Override
	public boolean exists(final N5Path normalPath) throws N5IOException {

		return isFile(normalPath) || isDirectory(normalPath);
	}

	@Override
	public long size(final N5FilePath normalPath) throws N5IOException {

		final String key = root.resolve(normalPath).path();

		if (!bucketExists())
			throw new N5NoSuchKeyException(key);

		try {
			final Blob blob = storage.get(BlobId.of(bucket, key), BlobGetOption.fields(BlobField.SIZE));
			if( blob == null )
				throw new N5NoSuchKeyException("No such key: " + key);
			return blob.getSize();
		} catch (StorageException e) {
			throw new N5IOException("Could not get key " + key, e);
		}
	}

	@Override
	public void write(final N5FilePath normalPath, final ReadData data) throws N5IOException {

		if (!bucketExists() && createBucket) { // TODO: revisit bucket creation logic
			createBucket();
		}

		final String key = root.resolve(normalPath).path();
		BlobInfo blobInfo = BlobInfo.newBuilder(bucket, key).build();
		try (OutputStream outputStream = Channels.newOutputStream(storage.writer(blobInfo))) {
			data.writeTo(outputStream);
		} catch (IOException | UncheckedIOException | StorageException e) {
			throw new N5IOException("Could not write to key " + key, e);
		}
	}

	@Override
	public String[] listDirectories(final N5DirectoryPath normalPath) throws N5IOException {

		final String prefix = root.resolve(normalPath).path();

		if (!bucketExists())
			throw new N5NoSuchKeyException(prefix);

		final List<String> subGroups = new ArrayList<>();
		try {
			final Page<Blob> blobListing = storage.list(
					bucket,
					BlobListOption.prefix(prefix),
					BlobListOption.currentDirectory(),
					BlobListOption.fields(BlobField.ID));

			blobListing.streamAll()
					.filter(Blob::isDirectory)
					.forEach(blob -> {
						final String pp = blob.getName();
						final String relativePath = pp.substring(prefix.length(), pp.length() - 1);
						if (!relativePath.isEmpty())
							subGroups.add(relativePath);
					});
		} catch (StorageException e) {
			throw new N5IOException("Could not list directory " + prefix, e);
		}

		if (subGroups.size() <= 0) {
			if(!isDirectory(normalPath))
				throw new N5NoSuchKeyException(normalPath + " is not a valid group");
		}

		return subGroups.toArray(new String[0]);
	}

	@Override
	public void createDirectories(final N5DirectoryPath normalPath) throws N5IOException {

		// If the bucket doesn't exist, and we should create it, then do so here.
		if (!bucketExists() && createBucket) { // TODO: revisit bucket creation logic
			createBucket();
		}

		final N5DirectoryPath group = root.resolve(normalPath).asDirectory();
		if (group.path().isEmpty()) // TODO (N5Path): should have N5DirectoryPath.isEmpty() ?
			return;

		String key = "";
		for (final String child : group.components()) {
			key += child + "/";
			final BlobInfo blobInfo = BlobInfo.newBuilder(bucket, key).build();
			storage.create(blobInfo);
		}
	}

	@Override
	public void delete(final N5Path normalPath) throws N5IOException {

		if (!bucketExists())
			return;

		final String key = root.resolve(normalPath).path();

		if (!normalPath.isDirectory())
			storage.delete(BlobId.of(bucket, key));

		/*
		 * TODO consider instead using Object Lifecycle Management when deleting many items see:
		 * https://cloud.google.com/storage/docs/deleting-objects#delete-objects-in-bulk
		 */
		Page<Blob> page = storage.list(
				bucket,
				BlobListOption.prefix(key),
				BlobListOption.fields(BlobField.ID));

		while (page != null) {
			final BlobId[] ids = page.streamValues().map(Blob::getBlobId).toArray(BlobId[]::new);
			if (ids.length > 0) // storage throws an error if ids is empty
				storage.delete(ids);
			page = page.getNextPage();
		}

		/* remove bucket when deleting the root "/"
		 * this needs to happen at the end because a bucket must be empty before it is deleted
		 *
		 * Buckets cannot be removed here if Object Lifecycle Management is used to delete objects.
		 */
		if (key.isEmpty() || key.equals("/")) {
			deleteBucket();
		}
	}


	//
	// ------------------------------------------------------------------------
	//


	class GCSLazyRead implements LazyRead {

		private final String key;
		private volatile Long generation = null;

		GCSLazyRead(final String key) {
			this.key = key;
		}

		private Blob getBlob(BlobGetOption... options) {
			final Blob blob;
			try {
				if (generationMatch && generation != null) {

					final BlobGetOption[] generationMatchOptions = Arrays.copyOf(options, options.length + 1);
					generationMatchOptions[options.length] = BlobGetOption.generationMatch(generation);

					BlobId blobId = BlobId.of(bucket, key);
					blob = storage.get(blobId, generationMatchOptions);
				} else {
					BlobId blobId = BlobId.of(bucket, key);
					blob = storage.get(blobId, options);
				}
			} catch (StorageException e) {
				if (e.getCode() == 404)
					throw new N5NoSuchKeyException("No such key. bucket: " + bucket + ". key: " + key);
				if (e.getCode() == 412)
					throw new N5ConcurrentModificationException("Generation mismatch. bucket: " + bucket + ". key: " + key);
				throw e;
			}

			if (blob == null)
				throw new N5NoSuchKeyException("No such key. bucket: " + bucket + ". key: " + key);

			if (generationMatch && generation == null)
				generation = blob.getGeneration();

			return blob;
		}

		@Override
		public long size() {

			final Blob blob = getBlob(BlobGetOption.fields(BlobField.SIZE, BlobField.GENERATION));
			return blob.getSize();
		}

		@Override
		public ReadData materialize(final long offset, final long length) {

			if (length > Integer.MAX_VALUE)
				throw new N5IOException("Attempt to materialize too large data");

			final Blob blob = getBlob();
			try (ReadChannel from = blob.reader()) {

				final long channelSize = blob.getSize();
				LazyRead.validateBounds(channelSize, offset, length);

				from.seek(offset);
				if (length > 0)
					from.limit(offset + length);

				long readLength;
				if (length < 0)
					readLength = channelSize - offset;
				else
					readLength = length;

				final ByteBuffer buf = ByteBuffer.allocate((int) readLength);
				from.read(buf);
				return ReadData.from(buf);

			} catch (IOException e) {
				throw new N5IOException(e);
			}
		}

		@Override
		public void close() {
		}
	}
}
