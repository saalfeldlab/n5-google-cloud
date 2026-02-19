package org.janelia.saalfeldlab.n5.googlecloud;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobField;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Objects;

import org.janelia.saalfeldlab.googlecloud.GoogleCloudStorageURI;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudUtils;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class GoogleCloudStorageKeyValueAccess implements KeyValueAccess {

	private static final String NORMAL_ROOT = N5URI.normalizeGroupPath( "/" );

	/*
	 * Error codes
	 */
	final static int FAILED_PRECONDITION = 400;
	final static int INVALID_ARGUMENT = 401;
	final static int UNAUTHENTICATED = 402;
	final static int PERMISSION_DENIED = 403;
	final static int NOT_FOUND = 404;
	final static int ALREADY_EXISTS = 409;

	private final Storage storage;
	private final GoogleCloudStorageURI containerURI;
	public final String bucketName;
	private final GcsIoPolicy ioPolicy;


	private final boolean createBucket;
	private Boolean bucketCheckedAndExists = null;

	protected static GoogleCloudStorageURI uncheckedContainerLocationStringToGoogleURI(final String uri) {

		try {
			return new GoogleCloudStorageURI(uri);
		} catch (final Exception e) {
			throw new N5Exception("Container location " + uri + " is an invalid URI", e);
		}
	}

	/**
	 * Creates a {@link KeyValueAccess} using a google cloud storage backend.
	 *
	 * @param storage      the google cloud interface
	 * @param containerURI a string representation of a valid {@link URI } that points to the n5 container root.
	 * @param createBucket if true, a bucket will be created if it does not exist
	 * @throws N5IOException if the requested bucket does not exist and
	 *                                   createBucket is false
	 */
	public GoogleCloudStorageKeyValueAccess(final Storage storage, final String containerURI, final boolean createBucket) throws N5IOException {

		this(storage, uncheckedContainerLocationStringToGoogleURI(containerURI), createBucket);
	}

	/**
	 * Creates a {@link KeyValueAccess} using a google cloud storage backend.
	 *
	 * @param storage      the google cloud interface
	 * @param containerURI the root of the n5 container root.
	 * @param createBucket if true, a bucket will be created if it does not exist
	 * @throws N5IOException if the requested bucket does not exist and
	 *                                   createBucket is false
	 */
	public GoogleCloudStorageKeyValueAccess(final Storage storage, final URI containerURI, final boolean createBucket) throws N5IOException {

		this(storage, new GoogleCloudStorageURI(containerURI), createBucket);
	}


	/**
	 * Creates a {@link KeyValueAccess} using a google cloud storage backend.
	 *
	 * @param storage      the google cloud interface
	 * @param containerURI the root of the n5 container root.
	 * @param createBucket if true, a bucket will be created if it does not exist
	 * @throws N5IOException if the requested bucket does not exist and
	 *                                   createBucket is false
	 */
	public GoogleCloudStorageKeyValueAccess(final Storage storage, final GoogleCloudStorageURI containerURI, final boolean createBucket) throws N5IOException {

		this.storage = storage;
		this.containerURI = containerURI;
		this.bucketName = containerURI.getBucket();
		this.createBucket = createBucket;

		this.ioPolicy = setIoPolicy();
	}

	private GcsIoPolicy setIoPolicy() {

		String ioPolicy = System.getProperty("n5.ioPolicy");
		if (ioPolicy == null)
			return new GcsIoPolicy.GenerationMatch(storage, bucketName);

		switch (ioPolicy) {
			case "unsafe":
			case "atomicFallbackUnsafe": // For Gc, this is equivalent ot just Unsafe
				return new GcsIoPolicy.Unsafe(storage, bucketName);
			case "atomic":
			default:
				return new GcsIoPolicy.GenerationMatch(storage, bucketName);
		}
	}

	/**
	 * Checks if a bucket with the given name exists.
	 * <p>
	 * First asks the storage client if the bucket exists. That may fail due to insufficient permissions.
	 * In that case, attempt to list the root of that bucket which could succeed even if the previous check fails.
	 *
	 * @return true if the bucket exists
	 */
	public boolean bucketExists() {

		if (Objects.equal(bucketCheckedAndExists, true))
			return bucketCheckedAndExists;

		try {
			bucketCheckedAndExists = bucketExistsFromClient();
			return bucketCheckedAndExists;
		} catch( Exception e ) { }

		bucketCheckedAndExists = prefixExists("");
		return bucketCheckedAndExists;
	}

	private boolean bucketExistsFromClient() {

		final Bucket bucket = storage.get(bucketName);
		if (bucket == null)
			return false;

		return bucket.exists();
	}

	private boolean prefixExists(final String key) {

		// not every directory will have an empty "directory" key stored in the backend,
		// for example, if the container contents was copied to GCS with the cli
		// in that case, check if any keys exist with the prefix, if so, it's a directory
		try {
			return storage.list(bucketName,
							BlobListOption.prefix(key),
							BlobListOption.pageSize(1),
							BlobListOption.currentDirectory())
					.iterateAll().iterator().hasNext();
		} catch (final StorageException e) {
			if (e.getCode() == NOT_FOUND)
				return false;
			else throw e;
		}
	}

	private void createBucket() {

		if (!createBucket)
			throw new N5Exception("Create Bucket Not Allowed");

		if (!bucketExists()) {
			try {
				storage.create(BucketInfo.of(bucketName));
				bucketCheckedAndExists = true;
			} catch (Exception e) {
				throw new N5IOException("Could not create bucket " + bucketName, e);
			}
		}
	}

	private void deleteBucket() {

		if (!createBucket)
			throw new N5Exception("Delete Bucket Not Allowed");

		// Not pointless, flag is Boolean, not boolean, and could be `null`
		if (Objects.equal(bucketCheckedAndExists, false))
			return;

		storage.delete(bucketName);
		bucketCheckedAndExists = false;
	}

	@Override
	public String[] components(final String path) {


		/* If the path is a valid URI with a scheme then use it to get the key. Otherwise,
		 * use the path directly, assuming it's a path only */
		String key = path;
		try {
			final URI uri = N5URI.getAsUri(path);
			final String scheme = uri.getScheme();
			if (scheme != null && !scheme.isEmpty())
				key = GoogleCloudUtils.getGoogleCloudStorageKey(uri);
		} catch (Throwable ignore) {}

		return KeyValueAccess.super.components(key);
	}

	@Override
	public String relativize(final String path, final String base) {

		try {
			/* Must pass absolute path to `uri`. if it already is, this is redundant, and has no impact on the result.
			 * 	It's not true that the inputs are always referencing absolute paths, but it doesn't matter in this
			 * 	case, since we only care about the relative portion of `path` to `base`, so the result always
			 * 	ignores the absolute prefix anyway. */
			final URI baseAsUri = uri("/" + base);
			final URI pathAsUri = uri("/" + path);
			final URI relativeUri = baseAsUri.relativize(pathAsUri);
			return relativeUri.getPath();
		} catch (final URISyntaxException e) {
			throw new N5Exception("Cannot relativize path (" + path + ") with base (" + base + ")", e);
		}
	}

	@Override
	public String normalize(final String path) {

		return N5URI.normalizeGroupPath(path);
	}

	/**
	 * Create a URI that is the result of resolving the `normalPath` against the {@link #containerURI}.
	 * NOTE: {@link URI#resolve(URI)} always removes the last member of the receiver URIs path.
	 * That is undesirable behavior here, as we want to potentially keep the containerURI's
	 * full path, and just append `normalPath`. However, it's more complicated, as `normalPath`
	 * can also contain leading overlap with the trailing members of `containerURI.getPath()`.
	 * To properly resolve the two paths, we generate {@link Path}s from the results of {@link URI#getPath()}
	 * and use {@link Path#resolve(Path)}, which results in a guaranteed absolute path, with the
	 * desired path resolution behavior. That then is used to construct a new {@link URI}.
	 * Any query or fragment portions are ignored. Scheme and Authority are always
	 * inherited from {@link #containerURI}.
	 *
	 * @param normalPath EITHER a normalized path, or a valid URI
	 * @return the URI generated from resolving normalPath against containerURI
	 * @throws URISyntaxException if the given normal path is not a valid URI
	 */
	@Override
	public URI uri(final String normalPath) throws URISyntaxException {

		return KeyValueAccess.super.uri(compose(containerURI.asURI(), normalPath));
	}

	/**
	 * Test whether the {@code normalPath} exists.
	 * <p>
	 * Removes leading slash from {@code normalPath}, and then checks whether
	 * either {@code path} or {@code path + "/"} is a key.
	 *
	 * @param normalPath is expected to be in normalized form, no further
	 *                   efforts are made to normalize it.
	 * @return {@code true} if {@code path} exists, {@code false} otherwise
	 */
	@Override
	public boolean exists(final String normalPath) {

		return isFile(normalPath) || isDirectory(normalPath);
	}

	@Override
	public long size(final String normalPath) {

		final Blob blob = storage.get(BlobId.of(bucketName, normalPath), Storage.BlobGetOption.fields(BlobField.SIZE));
		return blob.getSize();
	}

	/**
	 * Check existence of the given {@code key}.
	 *
	 * @param key the object key
	 * @return {@code true} if {@code key} exists.
	 */
	private boolean keyExists(final String key) {

		final Blob blob = storage.get(BlobId.of(bucketName, key), Storage.BlobGetOption.fields());
		return blobExists(blob);
	}

	private static boolean blobExists(final Blob blob) {

		// TODO document this 
		return blob != null && blob.exists();
	}

	private static String addTrailingSlash(final String path) {

		return path.endsWith("/") ? path : path + "/";
	}

	private static String removeLeadingSlash(final String path) {

		return path.startsWith("/") ? path.substring(1) : path;
	}

	private static boolean isRoot( final String path ) {

		return N5URI.normalizeGroupPath( path ).equals( NORMAL_ROOT );
	}

	/**
	 * Test whether the path is a directory.
	 * <p>
	 * Appends trailing "/" to {@code normalPath} if there is none, removes
	 * leading "/", and then checks whether resulting {@code path} is a key.
	 *
	 * @param normalPath is expected to be in normalized form, no further
	 *                   efforts are made to normalize it.
	 * @return {@code true} if {@code path} (with trailing "/") exists as a key, {@code false} otherwise
	 */
	@Override
	public boolean isDirectory(final String normalPath) {

		final String pathKey = removeLeadingSlash(addTrailingSlash(GoogleCloudUtils.getGoogleCloudStorageKey(normalPath)));
		// The root existing is equivalent to checking if the bucket exists.
		if (isRoot(pathKey))
			return bucketExists();

		if (prefixExists(pathKey))
			return true;

		try {
			/*may be no children, but may have proper `directory` key*/
			final Blob blob = storage.get(bucketName, pathKey);
			if (blob != null)
				return blob.getSize() == 0;
		} catch (final Exception ignore) {}

		return false;
	}

	/**
	 * Test whether the path is a file.
	 * <p>
	 * Checks whether {@code normalPath} has no trailing "/", then removes
	 * leading "/" and checks whether the resulting {@code path} is a key.
	 *
	 * @param normalPath is expected to be in normalized form, no further
	 *                   efforts are made to normalize it.
	 * @return {@code true} if {@code path} exists as a key and has no trailing slash, {@code false} otherwise
	 */
	@Override
	public boolean isFile(final String normalPath) {

		final String key = GoogleCloudUtils.getGoogleCloudStorageKey(normalPath);
		return !key.endsWith("/") && keyExists(removeLeadingSlash(key));
	}
	@Override
	public VolatileReadData createReadData(String normalPath) {

		final String key = GoogleCloudUtils.getGoogleCloudStorageKey(normalPath);
		String normalKey = removeLeadingSlash(key);
		try {
			return ioPolicy.read(normalKey);
		} catch (IOException e) {
			throw new N5IOException(e);
		}
	}

	@Override
	public void write(final String normalPath, final ReadData data) throws N5IOException {

		final String key = GoogleCloudUtils.getGoogleCloudStorageKey(normalPath);
		String normalKey = removeLeadingSlash(key);
		try {
			ioPolicy.write(normalKey, data);
		} catch (IOException e) {
			throw new N5IOException(e);
		}
	}

	/**
	 * List all 'directory'-like children of a path.
	 *
	 * @param normalPath is expected to be in normalized form, no further
	 *                   efforts are made to normalize it.
	 * @return the array of child directories
	 */
	@Override
	public String[] listDirectories(final String normalPath) {

		return list(normalPath, true);
	}

	private String[] list(final String normalPath, final boolean onlyDirectories) {

		// TODO what should happen when listing a non-existent bucket / path?

		final String pathKey = GoogleCloudUtils.getGoogleCloudStorageKey(normalPath);
		final List<String> subGroups = new ArrayList<>();
		final String prefix = removeLeadingSlash(addTrailingSlash(pathKey));
		final Page<Blob> blobListing = storage.list(
				bucketName,
				BlobListOption.prefix(prefix),
				BlobListOption.currentDirectory(),
				BlobListOption.fields(BlobField.ID));
		int numBlobs = 0;
		for (final Iterator<Blob> blobIterator = blobListing.iterateAll().iterator(); blobIterator.hasNext(); numBlobs++) {
			final Blob nextBlob = blobIterator.next();
			final String blobName = nextBlob.getBlobId().getName();
			if (prefix.equals(blobName))
				continue;
			if (!onlyDirectories || blobName.endsWith("/")) {
				final String relativePath = normalize(relativize(blobName, prefix));
				if (!relativePath.isEmpty())
					subGroups.add(relativePath);
			}
		}
		if (numBlobs > 0)
			return subGroups.toArray(new String[0]);

		/* If no blobs, may still be an empty directory key. */
		try {
			final Blob blob = storage.get(bucketName, prefix);
			if (blob != null && blob.getSize() == 0)
				return new String[0];
		} catch (final Exception ignore) {}

		throw new N5IOException(normalPath + " is not a valid group");
	}

	@Override
	public String[] list(final String normalPath) {

		return list(normalPath, false);
	}

	@Override
	public void createDirectories(final String normalPath) {

		/* If the bucket doesn't exist, and we should create it, then do so here. */
		if (createBucket)
			createBucket();

		String path = "";
		for (final String component : components(removeLeadingSlash(normalPath))) {
			final String composed = addTrailingSlash(compose(path, component));
			if (composed.equals("/"))
				continue;

			path = composed;

			final BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, path).build();
			storage.create(blobInfo);
		}
	}

	@Override
	public void delete(final String normalPath) {

		if (!bucketExists())
			return;

		final String key = removeLeadingSlash(GoogleCloudUtils.getGoogleCloudStorageKey(normalPath));

        try {
            ioPolicy.delete(key);
        } catch (IOException e) {
            throw new N5IOException("Error deleting " + normalPath, e);
        }

        /* remove bucket when deleting the root "/"
		 * this needs to happen at the end because a bucket must be empty before it is deleted
		 *
		 * Buckets cannot be removed here if Object Lifecycle Management is used to delete objects.
		 */
		if (isRoot(key)) {
			deleteBucket();
		}
	}
}
