package org.janelia.saalfeldlab.googlecloud;

import java.net.URI;

public class GoogleCloudStorageURI
{
	private static final String googleCloudHost = "googleapis.com";
	private static final String storagePathPrefix = "/storage/v1/b/";

	private final String bucketName;
	private final String objectKey;

	public GoogleCloudStorageURI( final URI uri )
	{
		final String path;
		if ( uri.getScheme().equalsIgnoreCase( "gs" ) )
		{
			path = uri.getHost() + uri.getPath();
		}
		else if ( uri.getScheme().equalsIgnoreCase( "http" ) || uri.getScheme().equalsIgnoreCase( "https" ) )
		{
			if ( !uri.getHost().equalsIgnoreCase( googleCloudHost ) && !uri.getHost().equalsIgnoreCase( "www." + googleCloudHost ) )
				throw new IllegalArgumentException( "Not a google cloud storage link" );

			if ( !uri.getPath().toLowerCase().startsWith( storagePathPrefix ) )
				throw new IllegalArgumentException( "Not a google cloud storage link" );

			path = uri.getPath().substring( storagePathPrefix.length() );
		}
		else
		{
			throw new IllegalArgumentException( "Invalid scheme" );
		}

		final int delimeterIndex = path.indexOf( "/" );
		bucketName = path.substring( 0, delimeterIndex != -1 ? delimeterIndex : path.length() );
		objectKey = delimeterIndex != -1 && delimeterIndex < path.length() - 1 ? path.substring( delimeterIndex + 1 ) : null;
	}

	public String getBucket()
	{
		return bucketName;
	}

	public String getKey()
	{
		return objectKey;
	}
}
