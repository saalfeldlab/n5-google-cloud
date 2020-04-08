/**
 * License: GPL
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 2
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package org.janelia.saalfeldlab.n5.googlecloud.mock;

import org.janelia.saalfeldlab.n5.googlecloud.AbstractN5GoogleCloudStorageContainerPathTest;
import org.janelia.saalfeldlab.n5.googlecloud.N5GoogleCloudStorageWriter;
import org.junit.AfterClass;
import org.junit.Assert;

import java.io.IOException;

/**
 * Initiates testing of the Google Cloud Storage N5 implementation using mock library.
 * A non-trivial container path is used to create the test N5 container in the temporary bucket.
 *
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
public class N5GoogleCloudStorageContainerPathMockTest extends AbstractN5GoogleCloudStorageContainerPathTest {

    public N5GoogleCloudStorageContainerPathMockTest() throws IOException {

        super(MockGoogleCloudStorageFactory.getOrCreateStorage());
    }

    @AfterClass
    public static void cleanup() throws IOException {

        // override with more relaxed assertions because mock library does not support bucket creation and deletion
        rampDownAfterClass();
        Assert.assertNotNull(storage.get(testBucketName, "test/"));
        Assert.assertTrue(new N5GoogleCloudStorageWriter(storage, testBucketName).remove());
    }
}
