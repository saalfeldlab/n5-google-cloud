package org.janelia.saalfeldlab.googlecloud;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

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
import com.google.auth.oauth2.AccessToken;

public class GoogleCloudOAuth {

	public static interface Scope {

		@Override
		public String toString();

		public static Collection<String> toScopeStrings(final Collection<? extends Scope> scopes) {

			final List<String> scopeStrings = new ArrayList<>();
			for (final Scope scope : scopes)
				scopeStrings.add(scope.toString());
			return scopeStrings;
		}
	}

	/** Base directory to store user credentials. */
	private static final Path DATA_STORE_DIR = Paths.get(System.getProperty("user.home"), ".store");

	/** Global instance of the JSON factory. */
	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

	private final GoogleClientSecrets clientSecrets;
	private final AccessToken accessToken;
	private final String refreshToken;

	public GoogleCloudOAuth(
			final Collection<? extends Scope> scopes,
			final String credentialsPathName,
			final InputStream jsonClientSecretsResourceStream) throws IOException {

		final HttpTransport httpTransport;
		try {
			httpTransport = GoogleNetHttpTransport.newTrustedTransport();
		} catch (final GeneralSecurityException e) {
			throw new RuntimeException(e);
		}

		final File credentialsDir = DATA_STORE_DIR.resolve(credentialsPathName).toFile();
		final FileDataStoreFactory dataStoreFactory = new FileDataStoreFactory(credentialsDir);

		// load client secrets
		clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(jsonClientSecretsResourceStream));

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

	public GoogleClientSecrets getClientSecrets() {

		return clientSecrets;
	}

	public AccessToken getAccessToken() {

		return accessToken;
	}

	public String getRefreshToken() {

		return refreshToken;
	}
}
