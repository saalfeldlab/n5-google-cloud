package org.janelia.saalfeldlab.googlecloud;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;

import org.janelia.saalfeldlab.googlecloud.GoogleCloudClient.Scope;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudClientSecretsPrompt.GoogleCloudClientSecretsPromptReason;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudClientSecretsPrompt.GoogleCloudSecretsPromptCanceledException;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonGenerator;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.UserCredentials;

public class GoogleCloudOAuth {

	/** Base directory to store client secrets and user credentials. */
	public static final Path DATA_STORE_DIR = Paths.get(System.getProperty("user.home"), ".google", "n5-google-cloud");

	/** Filename to store client secrets. */
	private static final String CLIENT_SECRETS_FILENAME = ".client";

	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

	private final GoogleClientSecrets clientSecrets;
	private final AccessToken accessToken;
	private final String refreshToken;

	public GoogleCloudOAuth(final GoogleCloudClientSecretsPrompt clientSecretsPrompt) throws IOException {
		this(clientSecretsPrompt, CredentialProvider.DEFAULT_PROVIDER);
	}

	public GoogleCloudOAuth(final GoogleCloudClientSecretsPrompt clientSecretsPrompt, final CredentialProvider getCredential) throws IOException {

		this(
				Arrays.asList(
						GoogleCloudResourceManagerClient.ProjectsScope.READ_ONLY,
						GoogleCloudStorageClient.StorageScope.READ_WRITE
					),
				clientSecretsPrompt,
				getCredential
			);
	}

	public GoogleCloudOAuth(final Collection<? extends Scope> scopes, final GoogleCloudClientSecretsPrompt clientSecretsPrompt) throws IOException
	{
		this(scopes, clientSecretsPrompt, CredentialProvider.DEFAULT_PROVIDER);
	}

	public GoogleCloudOAuth(
			final Collection<? extends Scope> scopes,
			final GoogleCloudClientSecretsPrompt clientSecretsPrompt,
			CredentialProvider getCredential
			) throws IOException {

		final Path clientSecretsLocation = DATA_STORE_DIR.resolve(CLIENT_SECRETS_FILENAME);
		if (Files.exists(clientSecretsLocation)) {
			clientSecrets = loadClientSecrets(clientSecretsLocation);
		} else {
			GoogleClientSecrets temporarySecrets;
			try {
				temporarySecrets = clientSecretsPrompt.prompt(GoogleCloudClientSecretsPromptReason.NOT_FOUND);
				saveClientSecrets(clientSecretsLocation, temporarySecrets);
			} catch (final GoogleCloudSecretsPromptCanceledException e) {
				clientSecrets = null;
				accessToken = null;
				refreshToken = null;
				return;
			}
			clientSecrets = temporarySecrets;
		}

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

		// TODO: prompt for new client secret if the current one is invalid

		// authorize
		final Credential credential = getCredential.fromFlow( flow );

		accessToken = new AccessToken(credential.getAccessToken(), new Date(credential.getExpirationTimeMilliseconds()));
		refreshToken = credential.getRefreshToken();
	}

	public Credentials getCredentials() {

		final OAuth2Credentials credentials;
		if (clientSecrets == null || refreshToken == null) {
			credentials = accessToken != null ? OAuth2Credentials.create(accessToken) : null;
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

	protected static GoogleClientSecrets loadClientSecrets(final Path clientSecretsLocation) throws IOException {

		try (final Reader reader = new FileReader(clientSecretsLocation.toFile())) {
			return GoogleClientSecrets.load(JSON_FACTORY, reader);
		}
	}

	protected static void saveClientSecrets(final Path clientSecretsLocation, final GoogleClientSecrets clientSecrets) throws IOException {

		clientSecretsLocation.getParent().toFile().mkdirs();
		try (final Writer writer = new FileWriter(clientSecretsLocation.toFile())) {
			final JsonGenerator jsonWriter = JSON_FACTORY.createJsonGenerator(writer);
			jsonWriter.serialize(clientSecrets);
			jsonWriter.flush();
			jsonWriter.close();
		}
	}
}
