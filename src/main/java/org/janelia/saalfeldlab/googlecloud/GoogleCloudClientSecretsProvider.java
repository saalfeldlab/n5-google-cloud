package org.janelia.saalfeldlab.googlecloud;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Path;

import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.json.JsonGenerator;

public class GoogleCloudClientSecretsProvider {

	/** Filename to store client secrets. */
	public static final String CLIENT_SECRETS_FILENAME = ".client";

	/**
	 * Saves client secrets as a JSON file in the default location.
	 *
	 * @param clientId
	 * @param clientSecret
	 * @throws IOException
	 */
	public static void save(final GoogleClientSecrets clientSecrets) throws IOException {

		save(GoogleCloudOAuth.DATA_STORE_DIR.resolve(CLIENT_SECRETS_FILENAME), clientSecrets);
	}

	/**
	 * Saves client secrets as a JSON file in the specified location.
	 *
	 * @param location
	 * @param clientId
	 * @param clientSecret
	 * @throws IOException
	 */
	public static void save(final Path location, final GoogleClientSecrets clientSecrets) throws IOException {

		location.getParent().toFile().mkdirs();
		try (final Writer writer = new FileWriter(location.toFile())) {
			final JsonGenerator jsonWriter = GoogleCloudOAuth.JSON_FACTORY.createJsonGenerator(writer);
			jsonWriter.serialize(clientSecrets);
			jsonWriter.flush();
			jsonWriter.close();
		}
	}

	/**
	 * Loads client secrets from a JSON file in the default location.
	 *
	 * @throws IOException
	 */
	public static GoogleClientSecrets load() throws IOException {

		return load(GoogleCloudOAuth.DATA_STORE_DIR.resolve(CLIENT_SECRETS_FILENAME));
	}

	/**
	 * Loads client secrets from a JSON file in the specified location.
	 *
	 * @param location
	 * @throws IOException
	 */
	public static GoogleClientSecrets load(final Path location) throws IOException {

		try (final Reader reader = new FileReader(location.toFile())) {
			return GoogleClientSecrets.load(GoogleCloudOAuth.JSON_FACTORY, reader);
		}
	}
}
