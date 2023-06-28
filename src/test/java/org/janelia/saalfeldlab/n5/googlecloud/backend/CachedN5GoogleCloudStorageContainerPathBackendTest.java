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
package org.janelia.saalfeldlab.n5.googlecloud.backend;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.googlecloud.AbstractN5GoogleCloudStorageContainerPathTest;
import org.janelia.saalfeldlab.n5.googlecloud.N5GoogleCloudStorageWriter;

import com.google.gson.GsonBuilder;

/**
 * Initiates testing of the Google Cloud Storage N5 implementation using actual Google Cloud backend.
 * A non-trivial container path is used to create the test N5 container in the temporary bucket.
 *
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
public class CachedN5GoogleCloudStorageContainerPathBackendTest extends AbstractN5GoogleCloudStorageContainerPathTest {

    public CachedN5GoogleCloudStorageContainerPathBackendTest() {

        super(BackendGoogleCloudStorageFactory.getOrCreateStorage());
    }

	@Override protected N5Writer createN5Writer() throws IOException, URISyntaxException {

		final URI uri = new URI(tempN5Location());
		final String bucketName = uri.getHost();
		final String basePath = uri.getPath();
		return new N5GoogleCloudStorageWriter(storage, bucketName, basePath, new GsonBuilder(), true) {

			@Override public void close() {

				remove();
				super.close();
			}
		};
	}

}
