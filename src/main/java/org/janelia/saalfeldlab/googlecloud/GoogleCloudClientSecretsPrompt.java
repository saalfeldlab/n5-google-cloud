package org.janelia.saalfeldlab.googlecloud;

import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets.Details;

public abstract class GoogleCloudClientSecretsPrompt {

	public static enum GoogleCloudClientSecretsPromptReason {

		NOT_FOUND,
		INVALID
	}

	public static class GoogleCloudSecretsPromptCanceledException extends Exception {

		private static final long serialVersionUID = -7890773742351393519L;
	}

	public abstract GoogleClientSecrets prompt(final GoogleCloudClientSecretsPromptReason reason) throws GoogleCloudSecretsPromptCanceledException;

	public static GoogleClientSecrets create(final String clientId, final String clientSecret) {

		final GoogleClientSecrets googleClientSecrets = new GoogleClientSecrets();
		final Details googleClientSecretsDetails = new Details();
		googleClientSecretsDetails.setClientId(clientId);
		googleClientSecretsDetails.setClientSecret(clientSecret);
		googleClientSecrets.setInstalled(googleClientSecretsDetails);
		return googleClientSecrets;
	}
}
