package org.janelia.saalfeldlab.googlecloud;

import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;

public class GoogleCloudClientSecretsExceptionPrompt extends GoogleCloudClientSecretsPrompt {

	@Override
	public GoogleClientSecrets prompt(final GoogleCloudClientSecretsPromptReason reason) {

		switch (reason)
		{
		case NOT_FOUND:
			throw new RuntimeException("Google Cloud client secrets file does not exist.");
		case INVALID:
			throw new RuntimeException("Google Cloud client secrets are invalid.");
		default:
			throw new RuntimeException("Could not load Google Cloud client secrets.");
		}
	}
}
