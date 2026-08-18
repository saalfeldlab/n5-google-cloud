package org.janelia.saalfeldlab.n5.googlecloud;

import com.google.cloud.storage.Storage;
import org.janelia.saalfeldlab.n5.HierarchyStore;
import org.janelia.saalfeldlab.n5.KeyValueRoot;
import org.janelia.saalfeldlab.n5.KeyValueRootHierarchyStore;
import org.janelia.saalfeldlab.n5.cache.AbstractHierarchyCacheContractTest;
import org.janelia.saalfeldlab.n5.googlecloud.backend.BackendGoogleCloudStorageFactory;

public class GoogleCloudHierarchyCacheContractTest extends AbstractHierarchyCacheContractTest {

	@Override
	protected HierarchyStore createStore() {

		return new KeyValueRootHierarchyStore(newKeyValueRoot());
	}

	protected Storage getStorage() {

		return BackendGoogleCloudStorageFactory.getOrCreateStorage();
	}

	private KeyValueRoot newKeyValueRoot() {

		final String bucketURI = "gs://" + N5GoogleCloudStorageTests.tempBucketName();
		final String root = N5GoogleCloudStorageTests.tempContainerPath();
		return new GoogleCloudStorageKeyValueRoot(getStorage(), bucketURI, root, true);
	}
}
