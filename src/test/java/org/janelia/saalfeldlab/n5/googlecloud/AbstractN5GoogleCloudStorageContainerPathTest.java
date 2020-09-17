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
/**
 * License: GPL
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 2
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package org.janelia.saalfeldlab.n5.googlecloud;

import com.google.cloud.storage.Storage;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.junit.AfterClass;
import org.junit.Assert;

import java.io.IOException;

public abstract class AbstractN5GoogleCloudStorageContainerPathTest extends AbstractN5GoogleCloudStorageTest {

    protected static String testContainerPath = "/test/container/";

    public AbstractN5GoogleCloudStorageContainerPathTest(final Storage storage) {

        super(storage);
    }

    @Override
    protected N5Writer createN5Writer() throws IOException {

        return new N5GoogleCloudStorageWriter(storage, testBucketName, testContainerPath);
    }

    @AfterClass
    public static void cleanup() throws IOException {

        rampDownAfterClass();
        Assert.assertNotNull(storage.get(testBucketName));
        Assert.assertNull(storage.get(testBucketName, "test/"));
        new N5GoogleCloudStorageWriter(storage, testBucketName).remove();
        Assert.assertNull(storage.get(testBucketName));
    }
}
