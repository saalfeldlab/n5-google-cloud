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

import org.janelia.saalfeldlab.n5.googlecloud.AbstractN5GoogleCloudStorageBucketRootTest;

import java.io.IOException;

/**
 * Initiates testing of the Google Cloud Storage N5 implementation using mock library.
 * The test N5 container is created at the root of the new temporary bucket.
 *
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
public class N5GoogleCloudStorageBucketRootMockTest extends AbstractN5GoogleCloudStorageBucketRootTest {

    public N5GoogleCloudStorageBucketRootMockTest() throws IOException {

        super(MockGoogleCloudStorageFactory.getOrCreateStorage());
    }
}
