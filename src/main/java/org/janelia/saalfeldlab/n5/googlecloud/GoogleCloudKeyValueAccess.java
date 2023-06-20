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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
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
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5URI;

// TODO: rename to GoogleCloudStorageKeyValueAccess
public class GoogleCloudKeyValueAccess implements KeyValueAccess {

	private final Storage storage;
	private final String bucketName;

	/**
	 * TODO: javadoc
	 *
	 * @param storage
	 * @param bucketName
	 * @param createBucket
	 * @throws N5Exception.N5IOException
	 */
	public GoogleCloudKeyValueAccess(final Storage storage, final String bucketName, final boolean createBucket) throws N5Exception.N5IOException {

		this.storage = storage;
		this.bucketName = bucketName;

		if (!bucketExists(bucketName)) {
			if (createBucket) {
				storage.create(BucketInfo.of(bucketName));
			} else {
				throw new N5Exception.N5IOException("Bucket " + bucketName + " does not exist.");
			}
		}
	}

	private boolean bucketExists(final String bucketName) {
		final Bucket bucket = storage.get(bucketName);
		return (bucket != null && bucket.exists());
	}

	@Override
	public String[] components(final String path) {

		return Arrays.stream(path.split("/"))
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

	@Override
	public String parent(final String path) {

		final String[] components = components(path);
		final String[] parentComponents =Arrays.copyOf(components, components.length - 1);

		return compose(parentComponents);
	}

	@Override
	public String relativize(final String path, final String base) {

		try {
			/* Must pass absolute path to `uri`. if it already is, this is redundant, and has no impact on the result.
			 * 	It's not true that the inputs are always referencing absolute paths, but it doesn't matter in this
			 * 	case, since we only care about the relative portion of `path` to `base`, so the result always
			 * 	ignores the absolute prefix anyway. */
			return normalize(uri("/" + base).relativize(uri("/" + path)).getPath());
		} catch (URISyntaxException e) {
			throw new N5Exception("Cannot relativize path (" + path +") with base (" + base + ")", e);
		}
	}

	@Override
	public String normalize(final String path) {

		return N5URI.normalizeGroupPath(path);
	}

	@Override
	public URI uri(final String normalPath) throws URISyntaxException {

		return new URI("gs", bucketName, normalPath, null);
	}

	/**
	 * Test whether the {@code normalPath} exists.
	 * <p>
	 * Removes leading slash from {@code normalPath}, and then checks whether
	 * either {@code path} or {@code path + "/"} is a key.
	 *
	 * @param normalPath is expected to be in normalized form, no further
	 * 		efforts are made to normalize it.
	 * @return {@code true} if {@code path} exists, {@code false} otherwise
	 */
	@Override
	public boolean exists(final String normalPath) {

		return isDirectory(normalPath) || isFile(normalPath);
	}

	/**
	 * Check existence of the given {@code key}.
	 *
	 * @return {@code true} if {@code key} exists.
	 */
	private boolean keyExists(final String key) {

		final Blob blob = storage.get(BlobId.of(bucketName, key), Storage.BlobGetOption.fields());
		return blobExists(blob);
	}

	private static boolean blobExists(final Blob blob) {

		return blob != null && blob	.exists();
	}

	private static String addTrailingSlash(final String path) {

		return path.endsWith("/") ? path : path + "/";
	}

	private static String removeLeadingSlash(final String path) {

		return path.startsWith("/") ? path.substring(1) : path;
	}

	/**
	 * Test whether the path is a directory.
	 * <p>
	 * Appends trailing "/" to {@code normalPath} if there is none, removes
	 * leading "/", and then checks whether resulting {@code path} is a key.
	 *
	 * @param normalPath is expected to be in normalized form, no further
	 * 		efforts are made to normalize it.
	 * @return {@code true} if {@code path} (with trailing "/") exists as a key, {@code false} otherwise
	 */
	@Override
	public boolean isDirectory(final String normalPath) {

		final String key = removeLeadingSlash(addTrailingSlash(normalPath));
		return key.isEmpty() || keyExists(key);
	}

	/**
	 * Test whether the path is a file.
	 * <p>
	 * Checks whether {@code normalPath} has no trailing "/", then removes
	 * leading "/" and checks whether the resulting {@code path} is a key.
	 *
	 * @param normalPath is expected to be in normalized form, no further
	 * 		efforts are made to normalize it.
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
	 * 		efforts are made to normalize it.
	 * @return
	 * @throws IOException
	 */
	@Override
	public String[] listDirectories(final String normalPath) {

		return list(normalPath, true);
	}

	private String[] list(final String normalPath, final boolean onlyDirectories) {

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
		for (final Iterator<Blob> blobIterator = blobListing.iterateAll().iterator(); blobIterator.hasNext();) {
			final Blob nextBlob = blobIterator.next();
			final String blobName = nextBlob.getBlobId().getName();
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

		// remove bucket when deleting "/"
		if (normalPath.equals(normalize("/"))) {
			storage.delete(bucketName);
			return;
		}

		final String path = removeLeadingSlash(normalPath);

		if (!path.endsWith("/")) {
			storage.delete(BlobId.of(bucketName, path));
		}

		final String prefix = addTrailingSlash(path);
		Page<Blob> page = storage.list(
				bucketName,
				BlobListOption.prefix(prefix),
				BlobListOption.fields(BlobField.ID));

		page.iterateAll().forEach(Blob::delete);
		// TODO: it would be probably better to do this in batches but the mock api doesn't implement that ...
//		while (page != null) {
//			storage.delete(page.streamValues().map(Blob::getBlobId).toArray(BlobId[]::new));
//			page = page.getNextPage();
//		}
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

			final Blob blob = storage.get(BlobId.of(bucketName, path));
			if (!blobExists(blob))
				return null;
			final InputStream in = Channels.newInputStream(blob.reader());
			synchronized (resources) {
				resources.add(in);
			}
			return in;
		}

		@Override
		public Reader newReader() {

			final Blob blob = storage.get(BlobId.of(bucketName, path));
			if (!blobExists(blob))
				return null;
			final Reader in = Channels.newReader(blob.reader(), StandardCharsets.UTF_8.name());
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
	}
}
