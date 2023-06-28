package org.janelia.saalfeldlab.n5.googlecloud;

import com.google.cloud.storage.Storage;
import com.google.gson.GsonBuilder;

import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5KeyValueWriter;

/**
 * TODO: javadoc
 */
public class N5GoogleCloudStorageWriter extends N5KeyValueWriter {

	/**
	 * TODO: reduce number of constructors ?
	 */

	/**
	 * TODO: javadoc
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final String bucketName, final String basePath, final GsonBuilder gsonBuilder, final boolean cacheAttributes) throws N5Exception {

		super(
				new GoogleCloudStorageKeyValueAccess(storage, bucketName, true),
				basePath,
				gsonBuilder,
				cacheAttributes);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final String bucketName, final String basePath, final boolean cacheAttributes) throws N5Exception {

		this(storage, bucketName, basePath, new GsonBuilder(), cacheAttributes);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final String bucketName, final String basePath, final GsonBuilder gsonBuilder) throws N5Exception {

		this(storage, bucketName, basePath, gsonBuilder, false);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final String bucketName, final String basePath) throws N5Exception {

		this(storage, bucketName, basePath, new GsonBuilder());
	}

	/**
	 * TODO: javadoc
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final String bucketName, final GsonBuilder gsonBuilder, final boolean cacheAttributes) throws N5Exception {

		this(storage, bucketName, "/", gsonBuilder, cacheAttributes);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final String bucketName, final boolean cacheAttributes) throws N5Exception {

		this(storage, bucketName, "/", new GsonBuilder(), cacheAttributes);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final String bucketName, final GsonBuilder gsonBuilder) throws N5Exception {

		this(storage, bucketName, "/", gsonBuilder, false);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GoogleCloudStorageWriter(final Storage storage, final String bucketName) throws N5Exception {

		this(storage, bucketName, "/", new GsonBuilder());
	}
}
