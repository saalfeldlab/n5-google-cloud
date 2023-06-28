package org.janelia.saalfeldlab.n5.googlecloud;

import com.google.cloud.storage.Storage;
import com.google.gson.GsonBuilder;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5KeyValueReader;

/**
 * TODO: javadoc
 */
public class N5GoogleCloudStorageReader extends N5KeyValueReader {

	/**
	 * TODO: reduce number of constructors ?
	 */

	/**
	 * TODO: javadoc
	 */
	public N5GoogleCloudStorageReader(final Storage storage, final String bucketName, final String basePath, final GsonBuilder gsonBuilder, final boolean cacheMeta) throws N5Exception {

		super(
				new GoogleCloudStorageKeyValueAccess(storage, bucketName, false),
				basePath,
				gsonBuilder,
				cacheMeta);

		if( !exists("/"))
			throw new N5Exception.N5IOException("No container exists at " + basePath );
	}

	/**
	 * TODO: javadoc
	 */
	public N5GoogleCloudStorageReader(final Storage storage, final String bucketName, final String basePath, final boolean cacheMeta) throws N5Exception {

		this(storage, bucketName, basePath, new GsonBuilder(), cacheMeta);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GoogleCloudStorageReader(final Storage storage, final String bucketName, final String basePath, final GsonBuilder gsonBuilder) throws N5Exception {

		this(storage, bucketName, basePath, gsonBuilder, false);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GoogleCloudStorageReader(final Storage storage, final String bucketName, final String basePath) throws N5Exception {

		this(storage, bucketName, basePath, new GsonBuilder(), false);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GoogleCloudStorageReader(final Storage storage, final String bucketName, final GsonBuilder gsonBuilder, final boolean cacheMeta) throws N5Exception {

		this(storage, bucketName, "/", gsonBuilder, cacheMeta);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GoogleCloudStorageReader(final Storage storage, final String bucketName, final boolean cacheMeta) throws N5Exception {

		this(storage, bucketName, "/", new GsonBuilder(), cacheMeta);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GoogleCloudStorageReader(final Storage storage, final String bucketName, final GsonBuilder gsonBuilder) throws N5Exception {

		this(storage, bucketName, "/", gsonBuilder, false);
	}

	/**
	 * TODO: javadoc
	 */
	public N5GoogleCloudStorageReader(final Storage storage, final String bucketName) throws N5Exception {

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
