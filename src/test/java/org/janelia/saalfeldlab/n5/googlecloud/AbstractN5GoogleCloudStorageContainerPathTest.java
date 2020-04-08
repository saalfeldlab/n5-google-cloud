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
package org.janelia.saalfeldlab.n5.googlecloud;

import com.google.cloud.storage.Storage;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.junit.AfterClass;
import org.junit.Assert;

import java.io.IOException;

public abstract class AbstractN5GoogleCloudStorageContainerPathTest extends AbstractN5GoogleCloudStorageTest {

    protected static String testContainerPath = "/test/container/";

    public AbstractN5GoogleCloudStorageContainerPathTest(final Storage storage) {

        super(storage);
    }

    @Override
    protected N5Writer createN5Writer() throws IOException {

        return new N5GoogleCloudStorageWriter(storage, testBucketName, testContainerPath);
    }

    @AfterClass
    public static void cleanup() throws IOException {

        rampDownAfterClass();
        Assert.assertNotNull(storage.get(testBucketName));
        Assert.assertNotNull(storage.get(testBucketName, "test/"));
        new N5GoogleCloudStorageWriter(storage, testBucketName).remove();
        Assert.assertNull(storage.get(testBucketName));
    }
}
