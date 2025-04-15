package org.janelia.saalfeldlab.n5.googlecloud.mock;

import com.google.cloud.storage.Storage;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.googlecloud.N5GoogleCloudStorageTests;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.logging.Filter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class N5GoogleCloudStorageMockTest extends N5GoogleCloudStorageTests {

	public static class MissingAppNameLogFilter implements Filter {

		private static final String APP_NAME_LOG_MESSAGE = "Application name is not set. Call Builder#setApplicationName.";

		@Override public boolean isLoggable(LogRecord record) {

			if (Logger.getLogger(record.getLoggerName()).getLevel() == Level.FINEST)
				return true;
			return !APP_NAME_LOG_MESSAGE.equals(record.getMessage());
		}
	}

	@Override protected Storage getGoogleCloudStorage() {

		return MockGoogleCloudStorageFactory.getOrCreateStorage();
	}

	@Override public void testPathsWithIllegalUriCharacters() throws IOException, URISyntaxException {

		// NOTE: This is currently identical to the AbstractN5Test version, except we remove "%" from the array of illegalChars to test.
		// 	Specifically we ONLY remove it in this Mock version of the test. The actual backend has no problem, and the test passes
		//	just fine, but the mock version seems to have some incorrect handling of % in keys. Likely because % is a valid UTF-8 character
		//	but requires special encoding for URL over HTTP, and it seems the mock channels don't handle that case correctly
		try (N5Writer writer = createTempN5Writer()) {
			try (N5Reader reader = createN5Reader(writer.getURI().toString())) {

				final String[] illegalChars = {" ", "#"};
				for (final String illegalChar : illegalChars) {
					final String groupWithIllegalChar = "test" + illegalChar + "group";
					assertThrows("list over group should throw prior to create", N5Exception.N5IOException.class, () -> writer.list(groupWithIllegalChar));
					writer.createGroup(groupWithIllegalChar);
					assertTrue("Newly created group should exist", writer.exists(groupWithIllegalChar));
					assertArrayEquals("list over empty group should be empty list", new String[0], writer.list(groupWithIllegalChar));
					writer.setAttribute(groupWithIllegalChar, "/a/b/key1", "value1");
					final String attrFromWriter = writer.getAttribute(groupWithIllegalChar, "/a/b/key1", String.class);
					final String attrFromReader = reader.getAttribute(groupWithIllegalChar, "/a/b/key1", String.class);
					assertEquals("value1", attrFromWriter);
					assertEquals("value1", attrFromReader);


					final String datasetWithIllegalChar = "test" + illegalChar + "dataset";
					final DatasetAttributes datasetAttributes = new DatasetAttributes(dimensions, blockSize, DataType.UINT64, new RawCompression());
					writer.createDataset(datasetWithIllegalChar, datasetAttributes);
					final DatasetAttributes datasetFromWriter = writer.getDatasetAttributes(datasetWithIllegalChar);
					final DatasetAttributes datasetFromReader = reader.getDatasetAttributes(datasetWithIllegalChar);
					assertDatasetAttributesEquals(datasetAttributes, datasetFromWriter);
					assertDatasetAttributesEquals(datasetAttributes, datasetFromReader);
				}
			}
		}
	}

	@Override
	@Test public void testReaderCreation() throws IOException, URISyntaxException {

		try (N5Writer writer = createTempN5Writer()) {
			final String canonicalPath = writer.getURI().toString();

			final N5Reader n5r = createN5Reader(canonicalPath);
			assertNotNull(n5r);

			// existing directory without attributes is okay;
			// Remove and create to remove attributes store
			writer.removeAttribute("/", "/");
			final N5Reader na = createN5Reader(canonicalPath);
			assertNotNull(na);

			// existing location with attributes, but no version
			writer.removeAttribute("/", "/");
			writer.setAttribute("/", "mystring", "ms");
			final N5Reader wa = createN5Reader(canonicalPath);
			assertNotNull(wa);

			// existing directory with incompatible version should fail
			writer.removeAttribute("/", "/");
			writer.setAttribute("/", N5Reader.VERSION_KEY,
					new N5Reader.Version(N5Reader.VERSION.getMajor() + 1, N5Reader.VERSION.getMinor(), N5Reader.VERSION.getPatch()).toString());
			assertThrows("Incompatible version throws error", N5Exception.N5IOException.class,
					() -> createN5Reader(canonicalPath));
			writer.remove();
		}
		/* In the AbstractN5Test class, there is a final test to ensure the reader creation fails if the container doesn't exist.
		 * Unfortunately, the google cloud storage test framework doesn't support that during testing,
		 * so we cannot support it. If future cloud store testing frameworks support creating mock buckets, we can test then. */
	}
}
