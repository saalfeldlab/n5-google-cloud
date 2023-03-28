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
package org.janelia.saalfeldlab.n5.googlecloud.mock;

import java.io.IOException;

import com.google.gson.GsonBuilder;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.googlecloud.AbstractN5GoogleCloudStorageBucketRootTest;

import static org.junit.Assert.assertNotNull;

/**
 * Initiates testing of the Google Cloud Storage N5 implementation using mock library.
 * The test N5 container is created at the root of the new temporary bucket.
 *
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
public class N5GoogleCloudStorageBucketRootMockTest extends AbstractN5GoogleCloudStorageBucketRootTest {

    public N5GoogleCloudStorageBucketRootMockTest() throws IOException {

        super(MockGoogleCloudStorageFactory.getOrCreateStorage());
    }

    @Override protected N5Writer createN5Writer() throws IOException {

        return createTempMockWriter(storage, tempBucketName(), tempContainerPath(), new GsonBuilder());
    }

    @Override public void testReaderCreation() throws IOException {
        /* The Google cloud FakeStorageRpc that is used during tests does not support bucket creation.
         * It manages this by treating the storage as a single bucket, that is gauranteed to exist.
         * Because of this, we can't properly test the failure case where an N5GoogleClouseReader is
         * constructed over a bucket that does not exist (which should fail).
         *
         * We override the test to remove that particular test. */


        final String canonicalPath = tempBucketName();
        try (N5Writer writer = createN5Writer(canonicalPath)) {

            final N5Reader n5r = createN5Reader(canonicalPath);
            assertNotNull(n5r);

            // existing directory without attributes is okay;
            // Remove and create to remove attributes store
            writer.remove("/");
            writer.createGroup("/");
            final N5Reader na = createN5Reader(canonicalPath);
            assertNotNull(na);

            // existing location with attributes, but no version
            writer.remove("/");
            writer.createGroup("/");
            writer.setAttribute( "/", "mystring", "ms" );
            final N5Reader wa = createN5Reader( canonicalPath);
            assertNotNull( wa );

            /* For cleanup */
            writer.remove("/");
        }
    }
}
