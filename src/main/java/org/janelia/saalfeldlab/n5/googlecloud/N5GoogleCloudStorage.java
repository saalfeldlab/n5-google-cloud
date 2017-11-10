/**
 * Copyright (c) 2017, Stephan Saalfeld
 * All rights reserved.
 *
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
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5.googlecloud;

import java.io.IOException;

import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.GsonBuilder;

/**
 * Factory methods to create {@link N5Reader N5Readers} and {@link N5Writer N5Writers}.
 *
 * @author Igor Pisarev
 */
public interface N5GoogleCloudStorage {

	/**
	 * Opens an {@link N5Reader} using the default {@link Storage} client and a given bucket name.
	 *
	 * If the bucket does not exist, it will not be created and all
	 * subsequent attempts to read attributes, groups, or datasets will fail.
	 *
	 * @param bucketName
	 */
	public static N5Reader openCloudStorageReader(final String bucketName) {

		return openCloudStorageReader(StorageOptions.getDefaultInstance().getService(), bucketName);
	}

	/**
	 * Opens an {@link N5Writer} using the default {@link Storage} client and a given bucket name.
	 *
	 * If the bucket does not exist, it will be created.
	 *
	 * @param bucketName
	 * @throws IOException
	 */
	public static N5Writer openCloudStorageWriter(final String bucketName) throws IOException {

		return openCloudStorageWriter(StorageOptions.getDefaultInstance().getService(), bucketName);
	}

	/**
	 * Opens an {@link N5Writer} using a custom {@link Storage} client and a given bucket name.
	 *
	 * If the bucket does not exist, it will be created.
	 *
	 * @param storage
	 * @param bucketName
	 * @throws IOException
	 */
	public static N5Writer openCloudStorageWriter(final Storage storage, final String bucketName) throws IOException {

		return openCloudStorageWriter(storage, bucketName, new GsonBuilder());
	}

	/**
	 * Opens an {@link N5Reader} using a custom {@link Storage} client and a given bucket name.
	 *
	 * If the bucket does not exist, it will not be created and all
	 * subsequent attempts to read attributes, groups, or datasets will fail.
	 *
	 * @param storage
	 * @param bucketName
	 */
	public static N5Reader openCloudStorageReader(final Storage storage, final String bucketName) {

		return openCloudStorageReader(storage, bucketName, new GsonBuilder());
	}

	/**
	 * Opens an {@link N5Reader} using the default {@link Storage} client and a given bucket name
	 * with a custom {@link GsonBuilder} to support custom attributes.
	 *
	 * If the bucket does not exist, it will not be created and all
	 * subsequent attempts to read attributes, groups, or datasets will fail.
	 *
	 * @param bucketName
	 * @param gsonBuilder
	 */
	public static N5Reader openCloudStorageReader(final String bucketName, final GsonBuilder gsonBuilder) {

		return openCloudStorageReader(StorageOptions.getDefaultInstance().getService(), bucketName, gsonBuilder);
	}

	/**
	 * Opens an {@link N5Writer} using the default {@link Storage} client and a given bucket name
	 * with a custom {@link GsonBuilder} to support custom attributes.
	 *
	 * If the bucket does not exist, it will be created.
	 *
	 * @param bucketName
	 * @param gsonBuilder
	 * @throws IOException
	 */
	public static N5Writer openCloudStorageWriter(final String bucketName, final GsonBuilder gsonBuilder) throws IOException {

		return openCloudStorageWriter(StorageOptions.getDefaultInstance().getService(), bucketName, gsonBuilder);
	}

	/**
	 * Opens an {@link N5Reader} using a custom {@link Storage} client and a given bucket name
	 * with a custom {@link GsonBuilder} to support custom attributes.
	 *
	 * If the bucket does not exist, it will not be created and all
	 * subsequent attempts to read attributes, groups, or datasets will fail.
	 *
	 * @param storage
	 * @param bucketName
	 * @param gsonBuilder
	 */
	public static N5Reader openCloudStorageReader(final Storage storage, final String bucketName, final GsonBuilder gsonBuilder) {

		return new N5GoogleCloudStorageReader(storage, bucketName, gsonBuilder);
	}

	/**
	 * Opens an {@link N5Writer} using a custom {@link Storage} client and a given bucket name
	 * with a custom {@link GsonBuilder} to support custom attributes.
	 *
	 * If the bucket does not exist, it will be created.
	 *
	 * @param storage
	 * @param bucketName
	 * @param gsonBuilder
	 * @throws IOException
	 */
	public static N5Writer openCloudStorageWriter(final Storage storage, final String bucketName, final GsonBuilder gsonBuilder) throws IOException {

		return new N5GoogleCloudStorageWriter(storage, bucketName, gsonBuilder);
	}
}
