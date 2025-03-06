package org.janelia.saalfeldlab.n5.googlecloud;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobField;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Objects;

import org.janelia.saalfeldlab.googlecloud.GoogleCloudStorageURI;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudUtils;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5URI;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.nio.channels.NonReadableChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

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
	 * @throws N5Exception.N5IOException if the requested bucket does not exist and
	 *                                   createBucket is false
	 */
	public GoogleCloudStorageKeyValueAccess(final Storage storage, final String containerURI, final boolean createBucket) throws N5Exception.N5IOException {

		this(storage, uncheckedContainerLocationStringToGoogleURI(containerURI), createBucket);
	}

	/**
	 * Creates a {@link KeyValueAccess} using a google cloud storage backend.
	 *
	 * @param storage      the google cloud interface
	 * @param containerURI the root of the n5 container root.
	 * @param createBucket if true, a bucket will be created if it does not exist
	 * @throws N5Exception.N5IOException if the requested bucket does not exist and
	 *                                   createBucket is false
	 */
	public GoogleCloudStorageKeyValueAccess(final Storage storage, final URI containerURI, final boolean createBucket) throws N5Exception.N5IOException {

		this(storage, new GoogleCloudStorageURI(containerURI), createBucket);
	}


	/**
	 * Creates a {@link KeyValueAccess} using a google cloud storage backend.
	 *
	 * @param storage      the google cloud interface
	 * @param containerURI the root of the n5 container root.
	 * @param createBucket if true, a bucket will be created if it does not exist
	 * @throws N5Exception.N5IOException if the requested bucket does not exist and
	 *                                   createBucket is false
	 */
	public GoogleCloudStorageKeyValueAccess(final Storage storage, final GoogleCloudStorageURI containerURI, final boolean createBucket) throws N5Exception.N5IOException {

		this.storage = storage;
		this.containerURI = containerURI;
		this.bucketName = containerURI.getBucket();
		this.createBucket = createBucket;
	}

	/**
	 * Checks if a bucket with the given name exists.
	 * <p>
	 * First asks the storage client if the bucket exists. That may fail due to insufficient permissions.
	 * In that case, attempt to list the root of that bucket which could succeed even if the previous check fails.
	 * 
	 * @param bucketName the bucket
	 * @return true if the bucket exists
	 */
	public boolean bucketExists() {

		if (Objects.equal(bucketCheckedAndExists, true))
			return bucketCheckedAndExists;

		try {
			bucketCheckedAndExists = storage.get(bucketName) != null;
			return bucketCheckedAndExists;
		} catch( Exception e ) { }

		bucketCheckedAndExists = prefixExists("");
		return bucketCheckedAndExists;
	}

	private boolean prefixExists(final String key) {

		// not every directory will have a directly stored in the backend,
		// for example, if the container contents was copied to GCS with the cli
		// in that case, check if any keys exist with the prefix, if so, it's a directory
		try {
			return storage.list(bucketName,
							BlobListOption.prefix(key),
							BlobListOption.pageSize(1),
							BlobListOption.currentDirectory())
					.iterateAll().iterator().hasNext();
		} catch (final StorageException e) {
			if (e.getCode() == 404 && e.getMessage().equals("The specified bucket or key does not exist."))
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
				throw new N5Exception.N5IOException("Could not create bucket " + bucketName, e);
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

		final String[] baseComponents = path.split("/");
		if (baseComponents.length <= 1)
			return baseComponents;
		return Arrays.stream(baseComponents)
				.filter(x -> !x.isEmpty())
				.toArray(String[]::new);
	}

	@Override
	public String compose(final String... components) {

		if (components == null || components.length == 0)
			return null;

		return normalize(
				Arrays.stream(components)
						.filter(x -> !x.isEmpty())
						.collect(Collectors.joining("/"))
		);
	}

	/**
	 * Compose a path from a base uri and subsequent components.
	 *
	 * @param uri        the base path uri to resolve the components against
	 * @param components the components of the group path, relative to the n5 container
	 * @return the path
	 */
	@Override
	public String compose(final URI uri, final String... components) {

		final String[] uriComponents = new String[components.length + 1];
		System.arraycopy(components, 0, uriComponents, 1, components.length);
		uriComponents[0] = GoogleCloudUtils.getGoogleCloudStorageKey(uri);
		return compose(uriComponents);
	}

	@Override
	public String parent(final String path) {

		final String[] components = components(path);
		final String[] parentComponents = Arrays.copyOf(components, components.length - 1);

		return compose(parentComponents);
	}

	@Override
	public String relativize(final String path, final String base) {

		try {
			/* Must pass absolute path to `uri`. if it already is, this is redundant, and has no impact on the result.
			 * 	It's not true that the inputs are always referencing absolute paths, but it doesn't matter in this
			 * 	case, since we only care about the relative portion of `path` to `base`, so the result always
			 * 	ignores the absolute prefix anyway. */
			return GoogleCloudUtils.getGoogleCloudStorageKey(normalize(uri("/" + base).relativize(uri("/" + path)).getPath()));
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

		final URI asUri = containerURI.asURI();

		if (normalize(normalPath).equals(normalize("/")))
			return asUri;

		final Path containerPath = Paths.get(asUri.getPath());
		final Path givenPath = Paths.get(URI.create(normalPath).getPath());

		final Path resolvedPath = containerPath.resolve(givenPath);
		final String[] pathParts = new String[resolvedPath.getNameCount() + 1];
		pathParts[0] = "/";
		for (int i = 0; i < resolvedPath.getNameCount(); i++) {
			pathParts[i + 1] = resolvedPath.getName(i).toString();
		}
		final String normalResolvedPath = compose(pathParts);

		return new URI(asUri.getScheme(), asUri.getAuthority(), normalResolvedPath, null, null);

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

		return isDirectory(normalPath) || isFile(normalPath);
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

		final boolean isRoot = isRoot(normalPath);
		final String key = isRoot ? "" : 
			removeLeadingSlash(addTrailingSlash(normalPath));

		// The root existing is equivalent to checking if the bucket exists.
		if (isRoot)
			return bucketExists();
		else
			return prefixExists(key);
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

		return !normalPath.endsWith("/") && keyExists(removeLeadingSlash(normalPath));
	}

	@Override
	public LockedChannel lockForReading(final String normalPath) {

		return new GoogleCloudObjectChannel(removeLeadingSlash(normalPath), true);
	}

	@Override
	public LockedChannel lockForWriting(final String normalPath) {

		return new GoogleCloudObjectChannel(removeLeadingSlash(normalPath), false);
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

		if (!isDirectory(normalPath)) {
			throw new N5Exception.N5IOException(normalPath + " is not a valid group");
		}

		final List<String> subGroups = new ArrayList<>();
		final String prefix = removeLeadingSlash(addTrailingSlash(normalPath));
		final Page<Blob> blobListing = storage.list(
				bucketName,
				BlobListOption.prefix(prefix),
				BlobListOption.currentDirectory(),
				BlobListOption.fields(BlobField.ID));
		for (final Iterator<Blob> blobIterator = blobListing.iterateAll().iterator(); blobIterator.hasNext(); ) {
			final Blob nextBlob = blobIterator.next();
			final String blobName = nextBlob.getBlobId().getName();
			if (prefix.equals(blobName))
				continue;
			if (!onlyDirectories || blobName.endsWith("/")) {
				final String relativePath = relativize(blobName, prefix);
				if (!relativePath.isEmpty())
					subGroups.add(relativePath);
			}
		}
		return subGroups.toArray(new String[subGroups.size()]);
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
			path = addTrailingSlash(compose(path, component));
			if (path.equals("/")) {
				continue;
			}
			final BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, path).build();
			storage.create(blobInfo);
		}
	}

	@Override
	public void delete(final String normalPath) {

		if (!bucketExists())
			return;

		final String path = removeLeadingSlash(normalPath);

		if (!path.endsWith("/")) {
			storage.delete(BlobId.of(bucketName, path));
		}


		/*
		 * TODO consider instead using Object Lifecycle Management when deleting many items see:
		 * https://cloud.google.com/storage/docs/deleting-objects#delete-objects-in-bulk
		 */
		Page<Blob> page = storage.list(
				bucketName,
				BlobListOption.prefix(path),
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
		if (normalPath.equals(normalize("/"))) {
			deleteBucket();
			return;
		}
	}

	private class GoogleCloudObjectChannel implements LockedChannel {

		final String path;
		final boolean readOnly;
		final ArrayList<Closeable> resources = new ArrayList<>();

		GoogleCloudObjectChannel(final String path, final boolean readOnly) {

			this.path = path;
			this.readOnly = readOnly;
		}

		private void checkWritable() {

			if (readOnly) {
				throw new NonReadableChannelException();
			}
		}

		@Override
		public InputStream newInputStream() {

			final ReadChannel channel = storage.reader(bucketName, path);
			final InputStream in = new NoSuchKeyWrappedInputStream(Channels.newInputStream(channel));
			synchronized (resources) {
				resources.add(in);
			}
			return in;
		}

		@Override
		public Reader newReader() {

			final Reader in = new InputStreamReader(newInputStream(), StandardCharsets.UTF_8);
			synchronized (resources) {
				resources.add(in);
			}
			return in;
		}

		@Override
		public OutputStream newOutputStream() {

			checkWritable();
			final BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, path).build();
			final OutputStream out = Channels.newOutputStream(storage.writer(blobInfo));
			synchronized (resources) {
				resources.add(out);
			}
			return out;
		}

		@Override
		public Writer newWriter() {

			checkWritable();
			final BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, path).build();
			final Writer out = Channels.newWriter(storage.writer(blobInfo), StandardCharsets.UTF_8.name());
			synchronized (resources) {
				resources.add(out);
			}
			return out;
		}

		@Override
		public void close() throws IOException {

			synchronized (resources) {
				for (final Closeable resource : resources)
					resource.close();
				resources.clear();
			}
		}

		private class NoSuchKeyWrappedInputStream extends InputStream {

			private final InputStream in;

			public NoSuchKeyWrappedInputStream(InputStream in) {

				this.in = in;
			}

			private IOException rethrowOrNoSuchKeyException(IOException e) {

				if (e.getCause() instanceof StorageException && ((StorageException) e.getCause()).getCode() == 404)
					throw new N5Exception.N5NoSuchKeyException(e);
				return e;
			}

			@Override public int read() throws IOException {

				try {
					return in.read();
				} catch (final IOException e) {
					throw rethrowOrNoSuchKeyException(e);
				}
			}

			@Override public int read(byte[] b) throws IOException {

				try {
					return in.read(b);
				} catch (final IOException e) {
					throw rethrowOrNoSuchKeyException(e);
				}
			}

			@Override public int read(byte[] b, int off, int len) throws IOException {

				try {
					return in.read(b, off, len);
				} catch (final IOException e) {
					throw rethrowOrNoSuchKeyException(e);
				}
			}

			@Override public long skip(long n) throws IOException {

				try {
					return in.skip(n);
				} catch (final IOException e) {
					throw rethrowOrNoSuchKeyException(e);
				}
			}

			@Override public int available() throws IOException {

				try {
					return in.available();
				} catch (final IOException e) {
					throw rethrowOrNoSuchKeyException(e);
				}

			}

			@Override public void close() throws IOException {

				in.close();
			}

			@Override public void mark(int readlimit) {

				in.mark(readlimit);
			}

			@Override public void reset() throws IOException {

				try {
					in.reset();
				} catch (final IOException e) {
					throw rethrowOrNoSuchKeyException(e);
				}
			}

			@Override public boolean markSupported() {

				return in.markSupported();
			}
		}
	}
}
