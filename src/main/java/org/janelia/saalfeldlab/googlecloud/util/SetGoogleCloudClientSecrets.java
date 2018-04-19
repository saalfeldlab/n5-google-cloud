package org.janelia.saalfeldlab.googlecloud.util;

import java.io.IOException;

import org.janelia.saalfeldlab.googlecloud.GoogleCloudClientSecretsProvider;

import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets.Details;

public class SetGoogleCloudClientSecrets {

	public static void main(final String[] args) throws IOException {

		final String clientId = args[ 0 ], clientSecret = args[ 1 ];
		final GoogleClientSecrets googleClientSecrets = new GoogleClientSecrets();
		final Details googleClientSecretsDetails = new Details();
		googleClientSecretsDetails.setClientId(clientId);
		googleClientSecretsDetails.setClientSecret(clientSecret);
		googleClientSecrets.setInstalled(googleClientSecretsDetails);
		GoogleCloudClientSecretsProvider.save(googleClientSecrets);
	}
}
