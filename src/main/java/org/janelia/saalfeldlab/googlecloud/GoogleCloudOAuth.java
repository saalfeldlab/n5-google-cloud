package org.janelia.saalfeldlab.googlecloud;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.Date;

import org.janelia.saalfeldlab.googlecloud.GoogleCloudClient.Scope;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.UserCredentials;

public class GoogleCloudOAuth {

	/** Base directory to store client secrets and user credentials. */
	public static final Path DATA_STORE_DIR = Paths.get(System.getProperty("user.home"), ".google", "n5-google-cloud");

	/** Global instance of the JSON factory. */
	public static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

	private final GoogleClientSecrets clientSecrets;
	private final AccessToken accessToken;
	private final String refreshToken;

	public GoogleCloudOAuth(final Collection<? extends Scope> scopes, final GoogleClientSecrets clientSecrets) throws IOException {

		this.clientSecrets = clientSecrets;

		final HttpTransport httpTransport;
		try {
			httpTransport = GoogleNetHttpTransport.newTrustedTransport();
		} catch (final GeneralSecurityException e) {
			throw new RuntimeException(e);
		}

		final FileDataStoreFactory dataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR.toFile());

		// set up authorization code flow
		final GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
				httpTransport, JSON_FACTORY, clientSecrets, Scope.toScopeStrings(scopes))
				.setDataStoreFactory(dataStoreFactory)
				.setAccessType("offline")
				.setApprovalPrompt("force")
				.build();

		// authorize
		final Credential credential = new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");

		accessToken = new AccessToken(credential.getAccessToken(), new Date(credential.getExpirationTimeMilliseconds()));
		refreshToken = credential.getRefreshToken();
	}

	public Credentials getCredentials() {

		final OAuth2Credentials credentials;
		if (clientSecrets == null || refreshToken == null) {
			credentials = OAuth2Credentials.create(accessToken);
		} else {
			credentials = UserCredentials.newBuilder()
					.setAccessToken(accessToken)
					.setClientId(clientSecrets.getDetails().getClientId())
					.setClientSecret(clientSecrets.getDetails().getClientSecret())
					.setRefreshToken(refreshToken)
				.build();
		}
		return credentials;
	}
}
