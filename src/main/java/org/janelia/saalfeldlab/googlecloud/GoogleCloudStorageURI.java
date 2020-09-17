package org.janelia.saalfeldlab.googlecloud;

import java.net.URI;
import java.util.Map;

import com.google.common.base.Splitter;

public class GoogleCloudStorageURI
{
	private static final String googleCloudHost = "googleapis.com";
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

			if ( !uri.getHost().equalsIgnoreCase( googleCloudHost ) && !uri.getHost().equalsIgnoreCase( "www." + googleCloudHost ) )
				throw new IllegalArgumentException( "Not a google cloud storage link" );

			if ( !uri.getPath().toLowerCase().startsWith( storagePathPrefix ) )
				throw new IllegalArgumentException( "Not a google cloud storage link" );

			path = uri.getPath().substring( storagePathPrefix.length() );
			final int delimeterIndex = path.indexOf( "/" );

			bucketName = path.substring( 0, delimeterIndex != -1 ? delimeterIndex : path.length() );
			objectKey = delimeterIndex != -1 && delimeterIndex < path.length() - 1 ? path.substring( delimeterIndex ) : "";
			query = uri.getQuery();
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
