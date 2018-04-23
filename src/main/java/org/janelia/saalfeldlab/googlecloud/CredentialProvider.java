package org.janelia.saalfeldlab.googlecloud;

import java.io.IOException;

import com.google.api.client.auth.oauth2.AuthorizationCodeFlow;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;

/**
 *
 * Provide {@link Credential}. This interface allows callers to replace
 * {@link AuthorizationCodeInstalledApp} with their own provider (that
 * does not rely on awt).
 *
 * @author Philipp Hanslovsky
 *
 */
public interface CredentialProvider
{

	/**
	 *
	 * build {@link Credential from} {@link AuthorizationCodeFlow}
	 *
	 * @param flow
	 * @return
	 * @throws IOException
	 */
	public Credential fromFlow( AuthorizationCodeFlow flow ) throws IOException;

	/**
	 * Default fall-back to {@link AuthorizationCodeInstalledApp}.
	 */
	public static CredentialProvider DEFAULT_PROVIDER =
			flow -> new AuthorizationCodeInstalledApp( flow, new LocalServerReceiver() ).authorize( "user" );

}
