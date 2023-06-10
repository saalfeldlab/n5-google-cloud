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
package org.janelia.saalfeldlab.n5.googlecloud;

import java.net.URI;

import javax.annotation.Nullable;

import org.janelia.saalfeldlab.googlecloud.GoogleCloudStorageURI;
import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.N5KeyValueWriter;
import org.janelia.saalfeldlab.n5.N5Writer;

import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.BucketInfo.Builder;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.contrib.nio.CloudStorageConfiguration;
import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem;
import com.google.gson.GsonBuilder;

/**
 * N5 implementation using Google Cloud Storage backend with version compatibility check.
 *
 * @author Igor Pisarev
 */
public class N5GoogleCloudStorageWriter extends N5KeyValueWriter implements N5Writer {

	private static final String tryCreateBucket(
			final String bucketName,
			final CloudStorageConfiguration config,
			@Nullable final StorageOptions storageOptions,
			@Nullable final StorageClass storageClass,
			@Nullable final String location) {

		String projectId = storageOptions == null ? null : storageOptions.getProjectId();
		if (projectId == null)
			projectId = config.userProject();
//		if (projectId == null) {
//			final ResourceManager resourceManager = new GoogleCloudResourceManagerClient().create();
//			final Iterator<Project> projectsIterator = resourceManager.list().iterateAll().iterator();
//			if (projectId == null) {
//				if (!projectsIterator.hasNext())
//					projectId = null;
//				else
//					projectId = projectsIterator.next().getProjectId();
//			}
//		}

		final Storage storage = storageOptions.getService();

		return createBucket(
				storage,
				bucketName,
				projectId,
				storageClass,
				location);

	}

	private static final String createBucket(
			final Storage storage,
			final String bucketName,
			final String projectId,
			@Nullable final StorageClass storageClass,
			@Nullable final String location) {

		if (storage.get(bucketName) == null) {
			final Builder builder = BucketInfo.newBuilder(bucketName);
			if (storageClass != null) builder.setStorageClass(storageClass);
			if (location != null) builder.setLocation(location);

			storage.create(builder.build());
		}
		return bucketName;
	}

	/**
	 * Opens an {@link N5GoogleCloudStorageReader} with a custom {@link GsonBuilder}
	 * to support custom attributes.
	 *
	 * @param bucketName
	 *            the Google Cloud bucket
	 * @param config
	 *            the Google Cloud Storage configuration
	 * @param config
	 *            the Google Cloud Storage options
	 * @param containerPath
	 *            N5 base path inside the bucket
	 * @param gsonBuilder
	 *            the gson builder
	 * @param cacheMeta
	 *            cache attributes and meta data
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes and other meta data that requires
	 *            accessing the store. This is most interesting for high latency
	 *            backends. Changes of cached attributes and meta data by an
	 *            independent writer on the same container will not be tracked.
	 * @throws
	 *
	 */
	public N5GoogleCloudStorageWriter(
			final String bucketName,
			final CloudStorageConfiguration config,
			@Nullable final StorageOptions storageOptions,
			final String containerPath,
			final GsonBuilder gsonBuilder,
			final boolean cacheMeta) {

		super(
				new FileSystemKeyValueAccess(
						CloudStorageFileSystem.forBucket(
								tryCreateBucket(bucketName, config, storageOptions, null, null), config, storageOptions)),
				containerPath,
				gsonBuilder,
				cacheMeta);

		if (!exists("/"))
			createGroup("/");

		if (!VERSION.equals(getVersion()))
			setAttribute("/", VERSION_KEY, VERSION.toString());
	}

	/**
	 * Opens an {@link N5GoogleCloudStorageWriter} with a custom {@link GsonBuilder}
	 * to support custom attributes.
	 *
	 * @param googleCloudStorageURI
	 *            the Google Cloud Storage URI
	 * @param gsonBuilder
	 *            the gson builder
	 * @param cacheMeta
	 *            cache attributes and meta data
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes and other meta data that requires
	 *            accessing the store. This is most interesting for high latency
	 *            backends. Changes of cached attributes and meta data by an
	 *            independent writer on the same container will not be tracked.
	 */
	public N5GoogleCloudStorageWriter(
			final GoogleCloudStorageURI googleCloudStorageURI,
			final GsonBuilder gsonBuilder,
			final boolean cacheMeta) {

		this(
				googleCloudStorageURI.getBucket(),
				CloudStorageConfiguration.builder().userProject(googleCloudStorageURI.getProject()).build(),
				null,
				googleCloudStorageURI.getKey(),
				gsonBuilder,
				cacheMeta);
	}

	/**
	 * Opens an {@link N5GoogleCloudStorageWriter} with a custom {@link GsonBuilder}
	 * to support custom attributes.
	 *
	 * @param storageURI
	 *            the Google Cloud Storage URI
	 * @param gsonBuilder
	 *            the gson builder
	 * @param cacheMeta
	 *            cache attributes and meta data
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes and other meta data that requires
	 *            accessing the store. This is most interesting for high latency
	 *            backends. Changes of cached attributes and meta data by an
	 *            independent writer on the same container will not be tracked.
	 */
	public N5GoogleCloudStorageWriter(
			final URI storageURI,
			final GsonBuilder gsonBuilder,
			final boolean cacheMeta) {

		this(
				new GoogleCloudStorageURI(storageURI),
				gsonBuilder,
				cacheMeta);
	}

	/**
	 * Opens an {@link N5GoogleCloudStorageWriter} with a custom {@link GsonBuilder}
	 * to support custom attributes.
	 *
	 * @param storageURI
	 *            the Google Cloud Storage URI
	 * @param gsonBuilder
	 *            the gson builder
	 * @param cacheMeta
	 *            cache attributes and meta data
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes and other meta data that requires
	 *            accessing the store. This is most interesting for high latency
	 *            backends. Changes of cached attributes and meta data by an
	 *            independent writer on the same container will not be tracked.
	 */
	public N5GoogleCloudStorageWriter(
			final String storageURI,
			final GsonBuilder gsonBuilder,
			final boolean cacheMeta) {

		this(
				new GoogleCloudStorageURI(storageURI),
				gsonBuilder,
				cacheMeta);
	}

	/**
	 * Opens an {@link N5GoogleCloudStorageWriter} with a custom {@link GsonBuilder}
	 * to support custom attributes.
	 *
	 * @param bucketName
	 *            the Google Cloud bucket
	 * @param containerPath
	 *            N5 base path inside the bucket
	 * @param gsonBuilder
	 *            the gson builder
	 * @param cacheMeta
	 *            cache attributes and meta data
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes and other meta data that requires
	 *            accessing the store. This is most interesting for high latency
	 *            backends. Changes of cached attributes and meta data by an
	 *            independent writer on the same container will not be tracked.
	 */
	public N5GoogleCloudStorageWriter(
			final String bucketName,
			final String containerPath,
			final GsonBuilder gsonBuilder,
			final boolean cacheMeta) {

		this(
				bucketName,
				CloudStorageConfiguration.DEFAULT,
				null,
				containerPath,
				gsonBuilder,
				cacheMeta);
	}

	/**
	 * Opens an {@link N5GoogleCloudStorageWriter} with a custom {@link GsonBuilder}
	 * to support custom attributes.
	 *
	 * @param bucketName
	 *            the Google Cloud bucket
	 * @param containerPath
	 *            N5 base path inside the bucket
	 * @param gsonBuilder
	 *            the gson builder
	 */
	public N5GoogleCloudStorageWriter(
			final String bucketName,
			final String containerPath,
			final GsonBuilder gsonBuilder) {

		this(
				bucketName,
				CloudStorageConfiguration.DEFAULT,
				null,
				containerPath,
				gsonBuilder,
				true);
	}

	/**
	 * Opens an {@link N5GoogleCloudStorageWriter} with a custom {@link GsonBuilder}
	 * to support custom attributes.
	 *
	 * @param bucketName
	 *            the Google Cloud bucket
	 * @param containerPath
	 *            N5 base path inside the bucket
	 * @param cacheMeta
	 *            cache attributes and meta data
	 *            Setting this to true avoids frequent reading and parsing of
	 *            JSON encoded attributes and other meta data that requires
	 *            accessing the store. This is most interesting for high latency
	 *            backends. Changes of cached attributes and meta data by an
	 *            independent writer on the same container will not be tracked.
	 */
	public N5GoogleCloudStorageWriter(
			final String bucketName,
			final String containerPath,
			final boolean cacheMeta) {

		this(
				bucketName,
				CloudStorageConfiguration.DEFAULT,
				null,
				containerPath,
				new GsonBuilder(),
				cacheMeta);
	}

	/**
	 * Opens an {@link N5GoogleCloudStorageWriter} with a custom {@link GsonBuilder}
	 * to support custom attributes.
	 *
	 * @param bucketName
	 *            the Google Cloud bucket
	 * @param containerPath
	 *            N5 base path inside the bucket
	 */
	public N5GoogleCloudStorageWriter(
			final String bucketName,
			final String containerPath) {

		this(
				bucketName,
				CloudStorageConfiguration.DEFAULT,
				null,
				containerPath,
				new GsonBuilder(),
				true);
	}
}
