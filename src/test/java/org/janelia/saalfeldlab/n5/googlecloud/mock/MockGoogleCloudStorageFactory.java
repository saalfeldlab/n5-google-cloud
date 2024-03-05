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
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.janelia.saalfeldlab.googlecloud.GoogleCloudClient;

import com.google.api.gax.paging.Page;
import com.google.cloud.Policy;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.HmacKey;
import com.google.cloud.storage.Notification;
import com.google.cloud.storage.NotificationInfo;
import com.google.cloud.storage.PostPolicyV4;
import com.google.cloud.storage.ServiceAccount;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;

public class MockGoogleCloudStorageFactory {

	private static Storage storage;

	public static Storage getOrCreateStorage() {

		if (storage == null) {

			// If the credentials are present in the system, the mock test still
			// prints the warning about using end-user credentials for some
			// reason. Call this method to suppress the warning.
			new GoogleCloudClient() {

				@Override
				public Object create() {

					return null;
				}
			};

			storage = new MockBuckets(LocalStorageHelper.getOptions().getService());
		}

		return storage;
	}

	public static class MockBuckets implements Storage {

		final Storage delegate;

		final Map<String, Bucket> buckets = new HashMap<>();

		static BiFunction<BucketInfo, Storage, Bucket> asBucket;

		private Bucket asBucket(final BucketInfo info) {

			synchronized (MockBuckets.class) {
				if (asBucket == null) {
					try {
						final Method m = BucketInfo.class.getDeclaredMethod("asBucket", Storage.class);
						m.setAccessible(true);
						asBucket = (b, s) -> {
							try {
								return (Bucket)m.invoke(b, s);
							} catch (IllegalAccessException | InvocationTargetException e) {
								throw new RuntimeException(e);
							}
						};
					} catch (final ReflectiveOperationException e) {
						e.printStackTrace();
					}

				}
				return asBucket.apply(info, this);
			}
		}

		MockBuckets(final Storage delegate) {

			this.delegate = delegate;
		}

		@Override
		public Bucket get(final String bucket, final BucketGetOption... options) {

			synchronized (buckets) {
				return buckets.get(bucket);
			}
		}

		@Override
		public Bucket create(final BucketInfo bucketInfo, final BucketTargetOption... options) {

			synchronized (buckets) {
				final String name = bucketInfo.getName();
				if (buckets.containsKey(name))
					throw new StorageException(0, "bucket \"" + name + "\" already exists");
				final Bucket bucket = asBucket(bucketInfo);
				buckets.put(name, bucket);
				return bucket;
			}
		}

		@Override
		public boolean delete(final String bucket, final BucketSourceOption... options) {

			synchronized (buckets) {
				return buckets.remove(bucket) != null;
			}
		}

		@Override
		public List<Boolean> delete(final BlobId... blobIds) {

			final List<Boolean> results = new ArrayList<>();
			for (final BlobId blobId : blobIds) {
				results.add(delegate.delete(blobId));
			}
			return results;
		}

		// -- forward to delegate --

		@Override
		public Blob create(final BlobInfo blobInfo, final BlobTargetOption... options) {

			return delegate.create(blobInfo, options);
		}

		@Override
		public Blob create(final BlobInfo blobInfo, final byte[] content, final BlobTargetOption... options) {

			return delegate.create(blobInfo, content, options);
		}

		@Override
		public Blob create(
				final BlobInfo blobInfo,
				final byte[] content,
				final int offset,
				final int length,
				final BlobTargetOption... options) {

			return delegate.create(blobInfo, content, offset, length, options);
		}

		@Override
		public Blob create(final BlobInfo blobInfo, final InputStream content, final BlobWriteOption... options) {

			return delegate.create(blobInfo, content, options);
		}

		@Override
		public Blob createFrom(
				final BlobInfo blobInfo,
				final Path path,
				final BlobWriteOption... options) throws IOException {

			return delegate.createFrom(blobInfo, path, options);
		}

		@Override
		public Blob createFrom(
				final BlobInfo blobInfo,
				final Path path,
				final int bufferSize,
				final BlobWriteOption... options) throws IOException {

			return delegate.createFrom(blobInfo, path, bufferSize, options);
		}

		@Override
		public Blob createFrom(
				final BlobInfo blobInfo,
				final InputStream content,
				final BlobWriteOption... options) throws IOException {

			return delegate.createFrom(blobInfo, content, options);
		}

		@Override
		public Blob createFrom(
				final BlobInfo blobInfo,
				final InputStream content,
				final int bufferSize,
				final BlobWriteOption... options) throws IOException {

			return delegate.createFrom(blobInfo, content, bufferSize, options);
		}

		@Override
		public Bucket lockRetentionPolicy(final BucketInfo bucket, final BucketTargetOption... options) {

			return delegate.lockRetentionPolicy(bucket, options);
		}

		@Override
		public Blob get(final String bucket, final String blob, final BlobGetOption... options) {

			return delegate.get(bucket, blob, options);
		}

		@Override
		public Blob get(final BlobId blob, final BlobGetOption... options) {

			return delegate.get(blob, options);
		}

		@Override
		public Blob get(final BlobId blob) {

			return delegate.get(blob);
		}

		@Override
		public Page<Bucket> list(final BucketListOption... options) {

			return delegate.list(options);
		}

		@Override
		public Page<Blob> list(final String bucket, final BlobListOption... options) {

			return delegate.list(bucket, options);
		}

		@Override
		public Bucket update(final BucketInfo bucketInfo, final BucketTargetOption... options) {

			return delegate.update(bucketInfo, options);
		}

		@Override
		public Blob update(final BlobInfo blobInfo, final BlobTargetOption... options) {

			return delegate.update(blobInfo, options);
		}

		@Override
		public Blob update(final BlobInfo blobInfo) {

			return delegate.update(blobInfo);
		}

		@Override
		public boolean delete(final String bucket, final String blob, final BlobSourceOption... options) {

			return delegate.delete(bucket, blob, options);
		}

		@Override
		public boolean delete(final BlobId blob, final BlobSourceOption... options) {

			return delegate.delete(blob, options);
		}

		@Override
		public boolean delete(final BlobId blob) {

			return delegate.delete(blob);
		}

		@Override
		public Blob compose(final ComposeRequest composeRequest) {

			return delegate.compose(composeRequest);
		}

		@Override
		public CopyWriter copy(final CopyRequest copyRequest) {

			return delegate.copy(copyRequest);
		}

		@Override
		public byte[] readAllBytes(final String bucket, final String blob, final BlobSourceOption... options) {

			return delegate.readAllBytes(bucket, blob, options);
		}

		@Override
		public byte[] readAllBytes(final BlobId blob, final BlobSourceOption... options) {

			return delegate.readAllBytes(blob, options);
		}

		@Override
		public StorageBatch batch() {

			return delegate.batch();
		}

		@Override
		public ReadChannel reader(final String bucket, final String blob, final BlobSourceOption... options) {

			return delegate.reader(bucket, blob, options);
		}

		@Override
		public ReadChannel reader(final BlobId blob, final BlobSourceOption... options) {

			return delegate.reader(blob, options);
		}

		@Override
		public void downloadTo(final BlobId blob, final Path path, final BlobSourceOption... options) {

			delegate.downloadTo(blob, path, options);
		}

		@Override
		public void downloadTo(final BlobId blob, final OutputStream outputStream, final BlobSourceOption... options) {

			delegate.downloadTo(blob, outputStream, options);
		}

		@Override
		public WriteChannel writer(final BlobInfo blobInfo, final BlobWriteOption... options) {

			return delegate.writer(blobInfo, options);
		}

		@Override
		public WriteChannel writer(final URL signedURL) {

			return delegate.writer(signedURL);
		}

		@Override
		public URL signUrl(
				final BlobInfo blobInfo,
				final long duration,
				final TimeUnit unit,
				final SignUrlOption... options) {

			return delegate.signUrl(blobInfo, duration, unit, options);
		}

		@Override
		public PostPolicyV4 generateSignedPostPolicyV4(
				final BlobInfo blobInfo,
				final long duration,
				final TimeUnit unit,
				final PostPolicyV4.PostFieldsV4 fields,
				final PostPolicyV4.PostConditionsV4 conditions,
				final PostPolicyV4Option... options) {

			return delegate.generateSignedPostPolicyV4(blobInfo, duration, unit, fields, conditions, options);
		}

		@Override
		public PostPolicyV4 generateSignedPostPolicyV4(
				final BlobInfo blobInfo,
				final long duration,
				final TimeUnit unit,
				final PostPolicyV4.PostFieldsV4 fields,
				final PostPolicyV4Option... options) {

			return delegate.generateSignedPostPolicyV4(blobInfo, duration, unit, fields, options);
		}

		@Override
		public PostPolicyV4 generateSignedPostPolicyV4(
				final BlobInfo blobInfo,
				final long duration,
				final TimeUnit unit,
				final PostPolicyV4.PostConditionsV4 conditions,
				final PostPolicyV4Option... options) {

			return delegate.generateSignedPostPolicyV4(blobInfo, duration, unit, conditions, options);
		}

		@Override
		public PostPolicyV4 generateSignedPostPolicyV4(
				final BlobInfo blobInfo,
				final long duration,
				final TimeUnit unit,
				final PostPolicyV4Option... options) {

			return null;
		}

		@Override
		public List<Blob> get(final BlobId... blobIds) {

			return delegate.get(blobIds);
		}

		@Override
		public List<Blob> get(final Iterable<BlobId> blobIds) {

			return delegate.get(blobIds);
		}

		@Override
		public List<Blob> update(final BlobInfo... blobInfos) {

			return delegate.update(blobInfos);
		}

		@Override
		public List<Blob> update(final Iterable<BlobInfo> blobInfos) {

			return delegate.update(blobInfos);
		}

		@Override
		public List<Boolean> delete(final Iterable<BlobId> blobIds) {

			return delegate.delete(blobIds);
		}

		@Override
		public Acl getAcl(final String bucket, final Acl.Entity entity, final BucketSourceOption... options) {

			return delegate.getAcl(bucket, entity, options);
		}

		@Override
		public Acl getAcl(final String bucket, final Acl.Entity entity) {

			return delegate.getAcl(bucket, entity);
		}

		@Override
		public boolean deleteAcl(final String bucket, final Acl.Entity entity, final BucketSourceOption... options) {

			return delegate.deleteAcl(bucket, entity, options);
		}

		@Override
		public boolean deleteAcl(final String bucket, final Acl.Entity entity) {

			return delegate.deleteAcl(bucket, entity);
		}

		@Override
		public Acl createAcl(final String bucket, final Acl acl, final BucketSourceOption... options) {

			return delegate.createAcl(bucket, acl, options);
		}

		@Override
		public Acl createAcl(final String bucket, final Acl acl) {

			return delegate.createAcl(bucket, acl);
		}

		@Override
		public Acl updateAcl(final String bucket, final Acl acl, final BucketSourceOption... options) {

			return delegate.updateAcl(bucket, acl, options);
		}

		@Override
		public Acl updateAcl(final String bucket, final Acl acl) {

			return delegate.updateAcl(bucket, acl);
		}

		@Override
		public List<Acl> listAcls(final String bucket, final BucketSourceOption... options) {

			return delegate.listAcls(bucket, options);
		}

		@Override
		public List<Acl> listAcls(final String bucket) {

			return delegate.listAcls(bucket);
		}

		@Override
		public Acl getDefaultAcl(final String bucket, final Acl.Entity entity) {

			return delegate.getDefaultAcl(bucket, entity);
		}

		@Override
		public boolean deleteDefaultAcl(final String bucket, final Acl.Entity entity) {

			return delegate.deleteDefaultAcl(bucket, entity);
		}

		@Override
		public Acl createDefaultAcl(final String bucket, final Acl acl) {

			return delegate.createDefaultAcl(bucket, acl);
		}

		@Override
		public Acl updateDefaultAcl(final String bucket, final Acl acl) {

			return delegate.updateDefaultAcl(bucket, acl);
		}

		@Override
		public List<Acl> listDefaultAcls(final String bucket) {

			return delegate.listDefaultAcls(bucket);
		}

		@Override
		public Acl getAcl(final BlobId blob, final Acl.Entity entity) {

			return delegate.getAcl(blob, entity);
		}

		@Override
		public boolean deleteAcl(final BlobId blob, final Acl.Entity entity) {

			return delegate.deleteAcl(blob, entity);
		}

		@Override
		public Acl createAcl(final BlobId blob, final Acl acl) {

			return delegate.createAcl(blob, acl);
		}

		@Override
		public Acl updateAcl(final BlobId blob, final Acl acl) {

			return delegate.updateAcl(blob, acl);
		}

		@Override
		public List<Acl> listAcls(final BlobId blob) {

			return delegate.listAcls(blob);
		}

		@Override
		public HmacKey createHmacKey(final ServiceAccount serviceAccount, final CreateHmacKeyOption... options) {

			return delegate.createHmacKey(serviceAccount, options);
		}

		@Override
		public Page<HmacKey.HmacKeyMetadata> listHmacKeys(final ListHmacKeysOption... options) {

			return delegate.listHmacKeys(options);
		}

		@Override
		public HmacKey.HmacKeyMetadata getHmacKey(final String accessId, final GetHmacKeyOption... options) {

			return delegate.getHmacKey(accessId, options);
		}

		@Override
		public void deleteHmacKey(final HmacKey.HmacKeyMetadata hmacKeyMetadata, final DeleteHmacKeyOption... options) {

			delegate.deleteHmacKey(hmacKeyMetadata, options);
		}

		@Override
		public HmacKey.HmacKeyMetadata updateHmacKeyState(
				final HmacKey.HmacKeyMetadata hmacKeyMetadata,
				final HmacKey.HmacKeyState state,
				final UpdateHmacKeyOption... options) {

			return delegate.updateHmacKeyState(hmacKeyMetadata, state, options);
		}

		@Override
		public Policy getIamPolicy(final String bucket, final BucketSourceOption... options) {

			return delegate.getIamPolicy(bucket, options);
		}

		@Override
		public Policy setIamPolicy(final String bucket, final Policy policy, final BucketSourceOption... options) {

			return delegate.setIamPolicy(bucket, policy, options);
		}

		@Override
		public List<Boolean> testIamPermissions(
				final String bucket,
				final List<String> permissions,
				final BucketSourceOption... options) {

			return delegate.testIamPermissions(bucket, permissions, options);
		}

		@Override
		public ServiceAccount getServiceAccount(final String projectId) {

			return delegate.getServiceAccount(projectId);
		}

		@Override
		public Notification createNotification(final String bucket, final NotificationInfo notificationInfo) {

			return delegate.createNotification(bucket, notificationInfo);
		}

		@Override
		public Notification getNotification(final String bucket, final String notificationId) {

			return delegate.getNotification(bucket, notificationId);
		}

		@Override
		public List<Notification> listNotifications(final String bucket) {

			return delegate.listNotifications(bucket);
		}

		@Override
		public boolean deleteNotification(final String bucket, final String notificationId) {

			return delegate.deleteNotification(bucket, notificationId);
		}

		@Override
		public StorageOptions getOptions() {

			return delegate.getOptions();
		}
	}
}
