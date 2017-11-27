package org.janelia.saalfeldlab.googlecloud;

import java.net.URI;

import org.junit.Assert;
import org.junit.Test;

public class GoogleCloudStorageURITest
{
	@Test
	public void test()
	{
		final String[] links = new String[] {
				"gs://test-bucket/test-dir/test-object.test",
				"http://googleapis.com/storage/v1/b/test-bucket/test-dir/test-object.test",
				"http://www.googleapis.com/storage/v1/b/test-bucket/test-dir/test-object.test",
				"https://googleapis.com/storage/v1/b/test-bucket/test-dir/test-object.test",
				"https://www.googleapis.com/storage/v1/b/test-bucket/test-dir/test-object.test"
			};
		for ( final String link : links )
		{
			final GoogleCloudStorageURI uri = new GoogleCloudStorageURI( URI.create( link ) );
			Assert.assertEquals( "test-bucket", uri.getBucket() );
			Assert.assertEquals( "test-dir/test-object.test", uri.getKey() );
		}
	}

	@Test
	public void testBucketOnly()
	{
		final String[] links = new String[] {
				"gs://test-bucket",
				"gs://test-bucket/",
				"http://googleapis.com/storage/v1/b/test-bucket",
				"http://googleapis.com/storage/v1/b/test-bucket/",
				"http://www.googleapis.com/storage/v1/b/test-bucket",
				"http://www.googleapis.com/storage/v1/b/test-bucket/",
				"https://googleapis.com/storage/v1/b/test-bucket",
				"https://googleapis.com/storage/v1/b/test-bucket/",
				"https://www.googleapis.com/storage/v1/b/test-bucket",
				"https://www.googleapis.com/storage/v1/b/test-bucket/",
			};
		for ( final String link : links )
		{
			final GoogleCloudStorageURI uri = new GoogleCloudStorageURI( URI.create( link ) );
			Assert.assertEquals( "test-bucket", uri.getBucket() );
			Assert.assertNull( uri.getKey() );
		}
	}
}
