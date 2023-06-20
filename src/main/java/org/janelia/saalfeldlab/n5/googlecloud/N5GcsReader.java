package org.janelia.saalfeldlab.n5.googlecloud;

import com.google.cloud.storage.Storage;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5KeyValueReader;

/**
 * TODO: javadoc
 */
// TODO rename
public class N5GcsReader extends N5KeyValueReader {

	/**
	 * TODO: reduce number of constructors ?
	 */

	/**
	 * TODO: javadoc
	 */
	public N5GcsReader(final Storage storage, final String bucketName, final String basePath, final GsonBuilder gsonBuilder, final boolean cacheMeta) throws IOException {

		super(
				new GoogleCloudKeyValueAccess(storage, bucketName, false),
				basePath,
				gsonBuilder,
				cacheMeta);

		if( !exists("/"))
			throw new N5Exception.N5IOException("No container exists at " + basePath );
	}

	/**
	 * TODO: javadoc
	 */
	public N5GcsReader(final Storage storage, final String bucketName, final String basePath, final boolean cacheMeta) throws IOException {

		this(storage, bucketName, basePath, new GsonBuilder(), cacheMeta);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GcsReader(final Storage storage, final String bucketName, final String basePath, final GsonBuilder gsonBuilder) throws IOException {

		this(storage, bucketName, basePath, gsonBuilder, false);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GcsReader(final Storage storage, final String bucketName, final String basePath) throws IOException {

		this(storage, bucketName, basePath, new GsonBuilder(), false);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GcsReader(final Storage storage, final String bucketName, final GsonBuilder gsonBuilder, final boolean cacheMeta) throws IOException {

		this(storage, bucketName, "/", gsonBuilder, cacheMeta);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GcsReader(final Storage storage, final String bucketName, final boolean cacheMeta) throws IOException {

		this(storage, bucketName, "/", new GsonBuilder(), cacheMeta);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GcsReader(final Storage storage, final String bucketName, final GsonBuilder gsonBuilder) throws IOException {

		this(storage, bucketName, "/", gsonBuilder, false);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GcsReader(final Storage storage, final String bucketName) throws IOException {

		this(storage, bucketName, "/", new GsonBuilder(), false);
	}


//	/**
//	 * Determines whether the current N5 container is stored at the root level of the bucket.
//	 *
//	 * @return
//	 */
//	protected boolean isContainerBucketRoot() {
//		return isContainerBucketRoot(containerPath);
//	}
//
//	protected static boolean isContainerBucketRoot(String containerPath) {
//		return removeLeadingSlash(containerPath).isEmpty();
//	}
}
