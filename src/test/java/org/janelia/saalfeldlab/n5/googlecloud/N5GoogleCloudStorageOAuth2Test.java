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

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.GsonAttributesParser;
import org.junit.BeforeClass;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.resourcemanager.Project;
import com.google.cloud.resourcemanager.ResourceManager;
import com.google.cloud.resourcemanager.ResourceManagerOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

/**
 * Initiates testing of the Google Cloud Storage N5 implementation with OAuth 2.0 authentication.
 *
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
public class N5GoogleCloudStorageOAuth2Test extends AbstractN5Test {

	static private String testBucketName = "test-bucket-" + UUID.randomUUID();

	/** Directory to store user credentials. */
	private static final File DATA_STORE_DIR = new File(System.getProperty("user.home"), ".store/n5-test-oauth2");

	/** OAuth 2.0 scopes. */
	private static final List<String> SCOPES = Arrays.asList(
			"https://www.googleapis.com/auth/cloudplatformprojects.readonly",
			"https://www.googleapis.com/auth/devstorage.read_write");

	/**
	 * Global instance of the {@link DataStoreFactory}. The best practice is to make it a single
	 * globally shared instance across your application.
	 */
	private static FileDataStoreFactory dataStoreFactory;

	/** Global instance of the HTTP transport. */
	private static HttpTransport httpTransport;

	/** Global instance of the JSON factory. */
	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

	/**
	 * @throws IOException
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws IOException {

		final Storage storage;
		try {
			storage = createClient();
		} catch (final GeneralSecurityException e) {
			throw new RuntimeException(e);
		}

		n5 = N5GoogleCloudStorage.openCloudStorageWriter(storage, testBucketName);
		n5Parser = (GsonAttributesParser)n5;

		AbstractN5Test.setUpBeforeClass();
	}

	private static Storage createClient() throws IOException, GeneralSecurityException {

		httpTransport = GoogleNetHttpTransport.newTrustedTransport();
		dataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR);

		final Credential credential = authorize();

		final AccessToken accessToken = new AccessToken(credential.getAccessToken(), null);

		// get project id to run tests
		final String projectId = getCloudProjectId(accessToken);

		// create custom client
		final Storage storage = StorageOptions
				.newBuilder()
				.setProjectId(projectId)
				.setCredentials(OAuth2Credentials.create(accessToken))
				.build()
				.getService();

		return storage;
	}

	/** Authorizes the installed application to access user's protected data. */
	private static Credential authorize() throws IOException {

		// load client secrets
		final GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY,
				new InputStreamReader(N5GoogleCloudStorageOAuth2Test.class.getResourceAsStream("/client_secrets.json")));
		if (clientSecrets.getDetails().getClientId().startsWith("Enter")
				|| clientSecrets.getDetails().getClientSecret().startsWith("Enter ")) {
			fail("Enter Client ID and Secret from https://code.google.com/apis/console/ "
					+ "into src/test/resources/client_secrets.json");
		}
		// set up authorization code flow
		final GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
				httpTransport, JSON_FACTORY, clientSecrets, SCOPES).setDataStoreFactory(
						dataStoreFactory).build();
		// authorize
		return new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
	}

	private static String getCloudProjectId(final AccessToken accessToken) {

		final ResourceManager resourceManager = ResourceManagerOptions
				.newBuilder()
				.setCredentials(OAuth2Credentials.create(accessToken))
				.build()
				.getService();

		// list project ids and select the first entry
		final Page<Project> projectsListing = resourceManager.list();
		final Iterator<Project> projectIterator = projectsListing.iterateAll().iterator();
		if (!projectIterator.hasNext())
			fail("No projects were found. Create a google cloud project first");
		return projectIterator.next().getProjectId();
	}
}
