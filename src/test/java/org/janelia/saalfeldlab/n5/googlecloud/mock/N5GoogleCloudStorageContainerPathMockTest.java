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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import com.google.gson.GsonBuilder;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.googlecloud.AbstractN5GoogleCloudStorageContainerPathTest;
import org.junit.AfterClass;

import static org.junit.Assert.assertNotNull;

/**
 * Initiates testing of the Google Cloud Storage N5 implementation using mock library.
 * A non-trivial container path is used to create the test N5 container in the temporary bucket.
 *
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
public class N5GoogleCloudStorageContainerPathMockTest extends AbstractN5GoogleCloudStorageContainerPathTest {

    public N5GoogleCloudStorageContainerPathMockTest() throws IOException {

        super(MockGoogleCloudStorageFactory.getOrCreateStorage());
    }

    @Override protected N5Writer createN5Writer() throws IOException {

        return createN5Writer(tempContainerPath());
    }

    @Override protected N5Writer createN5Writer(String location) throws IOException {


        cleanTemporaryBucket(location);
        return super.createN5Writer(location);
    }

    @Override protected N5Writer createN5Writer(String location, GsonBuilder gson) throws IOException {

        cleanTemporaryBucket(location);
        return super.createN5Writer(location, gson);
    }

    @Override protected N5Reader createN5Reader(String location, GsonBuilder gson) throws IOException {

        cleanTemporaryBucket(location);
        return super.createN5Reader(location, gson);
    }

    @AfterClass
    public static void cleanup() throws IOException {

        // override with more relaxed assertions because mock library does not support bucket creation and deletion
        rampDownAfterClass();
    }

    @Override public void testReaderCreation() throws IOException {
        /* The Google cloud FakeStorageRpc that is used during tests does not support bucket creation.
         * It manages this by treating the storage as a single bucket, that is gauranteed to exist.
         * Because of this, we can't properly test the failure case where an N5GoogleClouseReader is
         * constructed over a bucket that does not exist (which should fail).
         *
         * We override the test to remove that particular test. */


        final File tmpFile = Files.createTempDirectory("reader-create-test-").toFile();
        tmpFile.deleteOnExit();
        final String canonicalPath = tmpFile.getCanonicalPath();
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
        }
    }
}
