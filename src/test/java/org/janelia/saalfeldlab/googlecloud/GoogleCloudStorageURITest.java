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
				"https://www.googleapis.com/storage/v1/b/test-bucket/test-dir/test-object.test",
//				"https://storage.googleapis.com/example_multi-n5_bucket/test-dir/test-object.test"
			};

		for ( final String link : links )
		{
			final GoogleCloudStorageURI uri = new GoogleCloudStorageURI( link );
			Assert.assertEquals( "test-bucket", uri.getBucket() );
			Assert.assertEquals( "/test-dir/test-object.test", uri.getKey() );
			Assert.assertNull( "null query", uri.getQuery() );
		}
	}

	public void testQuery()
	{
		final String[] links = new String[] {
				"gs://test-bucket/test-dir/test-object.test?project=value",
				"http://googleapis.com/storage/v1/b/test-bucket/test-dir/test-object.test?project=value",
				"http://www.googleapis.com/storage/v1/b/test-bucket/test-dir/test-object.test?project=value",
				"https://googleapis.com/storage/v1/b/test-bucket/test-dir/test-object.test?project=value",
				"https://www.googleapis.com/storage/v1/b/test-bucket/test-dir/test-object.test?project=value",
//				"https://storage.googleapis.com/example_multi-n5_bucket/test-dir/test-object.test?project=value"
			};

		for ( final String link : links )
		{
			final GoogleCloudStorageURI uri = new GoogleCloudStorageURI( link );
			Assert.assertEquals( "project=value", uri.getQuery() );
			Assert.assertEquals( "value", uri.getProject() );
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
				"https://storage.googleapis.com/test-bucket"
			};

		for ( final String link : links )
		{
			final GoogleCloudStorageURI uri = new GoogleCloudStorageURI( link );
			Assert.assertEquals( "test-bucket", uri.getBucket() );
			Assert.assertTrue( "path empty or root", uri.getKey().isEmpty() || uri.getKey().equals( "/" ));
		}
	}
}
