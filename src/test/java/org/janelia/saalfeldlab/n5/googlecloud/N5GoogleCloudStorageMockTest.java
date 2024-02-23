package org.janelia.saalfeldlab.n5.googlecloud;

import com.google.cloud.storage.Storage;
import org.janelia.saalfeldlab.n5.googlecloud.mock.MockGoogleCloudStorageFactory;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

public class N5GoogleCloudStorageMockTest extends N5GoogleCloudStorageTest {

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> data() {

		return Arrays.asList(new Object[][]{
				{"mock google cloud, container at generated path", null, false},
				{"mock google cloud, container at generated path , cache attributes", null, true},
				{"mock google cloud, container at root", "/", false},
				{"mock google cloud, container at root with , cache attributes", "/", true}
		});
	}

	@Override protected Storage getGoogleCloudStorage() {

		return MockGoogleCloudStorageFactory.getOrCreateStorage();
	}
}
