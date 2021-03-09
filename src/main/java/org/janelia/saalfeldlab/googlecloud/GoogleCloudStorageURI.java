/*-
 * #%L
 * N5 Google Cloud
 * %%
 * Copyright (C) 2017 - 2020 Igor Pisarev, Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.googlecloud;

import java.net.URI;
import java.util.Map;

import com.google.common.base.Splitter;

public class GoogleCloudStorageURI
{
	private static final String storageHost = "storage.googleapis.com";
	private static final String googleCloudHost = "googleapis.com";
	private static final String googleCloudHost2 = "storage.cloud.google.com";

	private static final String storagePathPrefix = "/storage/v1/b/";
	private static final String projectKey = "project";

	private final String bucketName;
	private final String objectKey;
	private final String query;
	private Map< String, String > queryMap;

	public GoogleCloudStorageURI( final String str )
	{
		this( URI.create( str ) );
	}

	public GoogleCloudStorageURI( final URI uri )
	{
		final String path;
		if ( uri.getScheme().equalsIgnoreCase( "gs" ) )
		{
			bucketName = uri.getAuthority();
			objectKey = uri.getPath();
			query = uri.getQuery();
		}
		else if ( uri.getScheme().equalsIgnoreCase( "http" ) || uri.getScheme().equalsIgnoreCase( "https" ) )
		{
			final String host = uri.getHost();

			if( host.equalsIgnoreCase( googleCloudHost ) || host.equalsIgnoreCase( "www." +googleCloudHost ))
			{
				if ( !uri.getPath().toLowerCase().startsWith( storagePathPrefix ) )
					throw new IllegalArgumentException( "Not a google cloud storage link" );

				path = uri.getPath().substring( storagePathPrefix.length() );
				final int delimeterIndex = path.indexOf( "/" );

				bucketName = path.substring( 0, delimeterIndex != -1 ? delimeterIndex : path.length() );
				objectKey = delimeterIndex != -1 && delimeterIndex < path.length() - 1 ? path.substring( delimeterIndex ) : "";
				query = uri.getQuery();
			}
			else if( host.equalsIgnoreCase( storageHost ) || host.equalsIgnoreCase( googleCloudHost2 ))
			{
				path = uri.getPath().indexOf( '/' ) == 0 ? uri.getPath().substring( 1 ) : uri.getPath();
				final int delimeterIndex = path.indexOf( "/" );

				bucketName = path.substring( 0, delimeterIndex != -1 ? delimeterIndex : path.length() );
				objectKey = delimeterIndex != -1 && delimeterIndex < path.length() - 1 ? path.substring( delimeterIndex ) : "";
				query = uri.getQuery();
			}
			else
			{
				throw new IllegalArgumentException( "Not a google cloud storage link" );	
			}
		}
		else
		{
			throw new IllegalArgumentException( "Invalid scheme" );
		}
		queryMap = parseQuery();
	}

	public String getBucket()
	{
		return bucketName;
	}

	public String getKey()
	{
		return objectKey;
	}

	public String getQuery()
	{
		return query;
	}
	
	public String getProject()
	{
		if( queryMap != null && queryMap.containsKey( projectKey ))
			return queryMap.get( projectKey );

		return null;
	}

	private Map<String,String> parseQuery()
	{
		if( query != null )
			return Splitter.on( '&' ).trimResults().withKeyValueSeparator( '=' ).split( query );
		else
			return null;
	}

}
