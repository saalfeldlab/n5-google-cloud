package org.janelia.saalfeldlab.googlecloud;

import java.util.Scanner;

import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;

public class GoogleCloudClientSecretsCmdLinePrompt extends GoogleCloudClientSecretsPrompt {

	@Override
	public GoogleClientSecrets prompt(final GoogleCloudClientSecretsPromptReason reason) {

		System.out.println("To set up access to Google Cloud, follow these instructions to create client ID & client secret: "
				+ "https://github.com/saalfeldlab/n5-google-cloud/blob/master/README.md" + System.lineSeparator());

		final String clientId, clientSecret;

		try (final Scanner scanner = new Scanner(System.in)) {
			System.out.println("Enter Google Cloud client ID:");
			clientId = scanner.next();
			System.out.println("Enter Google Cloud client secret:");
			clientSecret= scanner.next();
		}

		return create(clientId, clientSecret);
	}
}
