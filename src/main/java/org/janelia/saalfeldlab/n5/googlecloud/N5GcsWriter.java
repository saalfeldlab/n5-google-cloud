package org.janelia.saalfeldlab.n5.googlecloud;

import com.google.cloud.storage.Storage;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import org.janelia.saalfeldlab.n5.N5KeyValueWriter;

/**
 * TODO: javadoc
 */
// TODO rename
public class N5GcsWriter extends N5KeyValueWriter {

	/**
	 * TODO: reduce number of constructors ?
	 */

	/**
	 * TODO: javadoc
	 */
	public N5GcsWriter(final Storage storage, final String bucketName, final String basePath, final GsonBuilder gsonBuilder, final boolean cacheAttributes) throws IOException {

		super(
				new GoogleCloudKeyValueAccess(storage, bucketName, true),
				basePath,
				gsonBuilder,
				cacheAttributes);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GcsWriter(final Storage storage, final String bucketName, final String basePath, final boolean cacheAttributes) throws IOException {

		this(storage, bucketName, basePath, new GsonBuilder(), cacheAttributes);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GcsWriter(final Storage storage, final String bucketName, final String basePath, final GsonBuilder gsonBuilder) throws IOException {

		this(storage, bucketName, basePath, gsonBuilder, false);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GcsWriter(final Storage storage, final String bucketName, final String basePath) throws IOException {

		this(storage, bucketName, basePath, new GsonBuilder());
	}

	/**
	 * TODO: javadoc
	 */
	public N5GcsWriter(final Storage storage, final String bucketName, final GsonBuilder gsonBuilder, final boolean cacheAttributes) throws IOException {

		this(storage, bucketName, "/", gsonBuilder, cacheAttributes);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GcsWriter(final Storage storage, final String bucketName, final boolean cacheAttributes) throws IOException {

		this(storage, bucketName, "/", new GsonBuilder(), cacheAttributes);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GcsWriter(final Storage storage, final String bucketName, final GsonBuilder gsonBuilder) throws IOException {

		this(storage, bucketName, "/", gsonBuilder, false);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GcsWriter(final Storage storage, final String bucketName) throws IOException {

		this(storage, bucketName, "/", new GsonBuilder());
	}
}
