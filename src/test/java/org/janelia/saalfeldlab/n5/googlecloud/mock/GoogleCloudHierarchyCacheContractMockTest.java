package org.janelia.saalfeldlab.n5.googlecloud.mock;

import com.google.cloud.storage.Storage;
import org.janelia.saalfeldlab.n5.googlecloud.GoogleCloudHierarchyCacheContractTest;

public class GoogleCloudHierarchyCacheContractMockTest extends GoogleCloudHierarchyCacheContractTest {

	@Override
	protected Storage getStorage() {

		return MockGoogleCloudStorageFactory.getOrCreateStorage();
	}
}
