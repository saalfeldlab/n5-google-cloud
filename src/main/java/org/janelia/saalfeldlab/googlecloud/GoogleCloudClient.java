package org.janelia.saalfeldlab.googlecloud;

import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.UserCredentials;

public abstract class GoogleCloudClient {

	private final AccessToken accessToken;
	private final GoogleClientSecrets clientSecrets;
	private final String refreshToken;

	public GoogleCloudClient(final AccessToken accessToken) {

		this(accessToken, null, null);
	}

	public GoogleCloudClient(final AccessToken accessToken, final GoogleClientSecrets clientSecrets, final String refreshToken) {

		this.accessToken = accessToken;
		this.clientSecrets = clientSecrets;
		this.refreshToken = refreshToken;
	}

	protected OAuth2Credentials getCredentials() {

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
