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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;

import org.janelia.saalfeldlab.googlecloud.GoogleCloudOAuth;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudResourceManagerClient;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudStorageClient;
import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.N5Writer;

import com.google.cloud.resourcemanager.Project;
import com.google.cloud.resourcemanager.ResourceManager;
import com.google.cloud.storage.Storage;

/**
 * Initiates testing of the Google Cloud Storage N5 implementation with OAuth 2.0 authentication.
 *
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
public class N5GoogleCloudStorageOAuth2Test extends AbstractN5Test {

	static private String testBucketName = "n5-test-" + UUID.randomUUID();

	/**
	 * @throws IOException
	 */
	@Override
	protected N5Writer createN5Writer() throws IOException {

		final GoogleCloudOAuth oauth = new GoogleCloudOAuth(
				Arrays.asList(
						GoogleCloudResourceManagerClient.ProjectsScope.READ_ONLY,
						GoogleCloudStorageClient.StorageScope.READ_WRITE
					),
				"n5-test-oauth2",
				N5GoogleCloudStorageOAuth2Test.class.getResourceAsStream("/client_secrets.json")
			);

		// query a list of user's projects first
		final ResourceManager resourceManager = new GoogleCloudResourceManagerClient(
				oauth.getAccessToken(),
				oauth.getClientSecrets(),
				oauth.getRefreshToken()
			).create();

		final Iterator<Project> projectsIterator = resourceManager.list().iterateAll().iterator();
		if (!projectsIterator.hasNext())
			fail("No projects were found. Create a google cloud project first");

		// get first project id to run tests
		final String projectId = projectsIterator.next().getProjectId();

		final Storage storage = new GoogleCloudStorageClient(
				oauth.getAccessToken(),
				oauth.getClientSecrets(),
				oauth.getRefreshToken()
			).create(projectId);

		return new N5GoogleCloudStorageWriter(storage, testBucketName);
	}
}
