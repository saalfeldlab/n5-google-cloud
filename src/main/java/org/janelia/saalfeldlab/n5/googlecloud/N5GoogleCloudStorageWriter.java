package org.janelia.saalfeldlab.n5.googlecloud;

import com.google.cloud.storage.Storage;
import com.google.gson.GsonBuilder;

import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5KeyValueWriter;
import org.janelia.saalfeldlab.n5.N5Reader;

public class N5GoogleCloudStorageWriter extends N5KeyValueWriter {

	/**
	 * TODO: reduce number of constructors ?
	 */

	/**
	 * Opens an {@link N5Reader} with a google cloud {@link Storage} storage backend.
     *
     * @param storage the google cloud storage instance
     * @param bucketName the bucket name
     * @param basePath the base path relative to the bucket root
     * @param gsonBuilder a GsonBuilder with custom configuration.
     * @param cacheAttributes
     *            cache attribute and meta data
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes and other meta data that requires
	 *            accessing the store. This i smost interesting for high latency
	 *            backends. Changes of cached attributes and meta data by an
	 *            independent writer will not be tracked.
	 * @throws N5Exception if the reader could not be created
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final String bucketName, final String basePath, final GsonBuilder gsonBuilder, final boolean cacheAttributes) throws N5Exception {

		super(
				new GoogleCloudStorageKeyValueAccess(storage, bucketName, true),
				basePath,
				gsonBuilder,
				cacheAttributes);
	}

	/**
	 * Opens an {@link N5Reader} with a google cloud {@link Storage} storage backend.
     *
     * @param storage the google cloud storage instance
     * @param bucketName the bucket name
     * @param basePath the base path relative to the bucket root
     * @param cacheAttributes
     *            cache attribute and meta data
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes and other meta data that requires
	 *            accessing the store. This i smost interesting for high latency
	 *            backends. Changes of cached attributes and meta data by an
	 *            independent writer will not be tracked.
	 * @throws N5Exception if the reader could not be created
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final String bucketName, final String basePath, final boolean cacheAttributes) throws N5Exception {

		this(storage, bucketName, basePath, new GsonBuilder(), cacheAttributes);
	}

	/**
	 * Opens an {@link N5Reader} with a google cloud {@link Storage} storage backend.
	 * <p>
	 * Metadata are not cached.
     *
     * @param storage the google cloud storage instance
     * @param bucketName the bucket name
     * @param basePath the base path relative to the bucket root
     * @param gsonBuilder a GsonBuilder with custom configuration.
	 * @throws N5Exception if the reader could not be created
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final String bucketName, final String basePath, final GsonBuilder gsonBuilder) throws N5Exception {

		this(storage, bucketName, basePath, gsonBuilder, false);
	}

	/**
	 * Opens an {@link N5Reader} with a google cloud {@link Storage} storage backend.
	 * <p>
	 * Metadata are not cached.
     *
     * @param storage the google cloud storage instance
     * @param bucketName the bucket name
     * @param basePath the base path relative to the bucket root
	 * @throws N5Exception if the reader could not be created
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final String bucketName, final String basePath) throws N5Exception {

		this(storage, bucketName, basePath, new GsonBuilder());
	}

	/**
	 * Opens an {@link N5Reader} with a google cloud {@link Storage} storage backend.
	 * <p>
	 * The n5 container root is the bucket's root.
     *
     * @param storage the google cloud storage instance
     * @param bucketName the bucket name
     * @param gsonBuilder a GsonBuilder with custom configuration.
     * @param cacheAttributes
     *            cache attribute and meta data
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes and other meta data that requires
	 *            accessing the store. This i smost interesting for high latency
	 *            backends. Changes of cached attributes and meta data by an
	 *            independent writer will not be tracked.
	 * @throws N5Exception if the reader could not be created
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final String bucketName, final GsonBuilder gsonBuilder, final boolean cacheAttributes) throws N5Exception {

		this(storage, bucketName, "/", gsonBuilder, cacheAttributes);
	}

	/**
	 * Opens an {@link N5Reader} with a google cloud {@link Storage} storage backend.
	 * <p>
	 * The n5 container root is the bucket's root. Metadata are not cached.
     *
     * @param storage the google cloud storage instance
     * @param bucketName the bucket name
	 * @throws N5Exception if the reader could not be created
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final String bucketName, final boolean cacheAttributes) throws N5Exception {

		this(storage, bucketName, "/", new GsonBuilder(), cacheAttributes);
	}

	/**
	 * Opens an {@link N5Reader} with a google cloud {@link Storage} storage backend.
	 * <p>
	 * The n5 container root is the bucket's root. Metadata are not cached.
     *
     * @param storage the google cloud storage instance
     * @param bucketName the bucket name
     * @param gsonBuilder a GsonBuilder with custom configuration.
	 * @throws N5Exception if the reader could not be created
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final String bucketName, final GsonBuilder gsonBuilder) throws N5Exception {

		this(storage, bucketName, "/", gsonBuilder, false);
	}

	/**
	 * Opens an {@link N5Reader} with a google cloud {@link Storage} storage backend.
	 * <p>
	 * The n5 container root is the bucket's root. Metadata are not cached.
     *
     * @param storage the google cloud storage instance
     * @param bucketName the bucket name
	 * @throws N5Exception if the reader could not be created
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final String bucketName) throws N5Exception {

		this(storage, bucketName, "/", new GsonBuilder());
	}
}
