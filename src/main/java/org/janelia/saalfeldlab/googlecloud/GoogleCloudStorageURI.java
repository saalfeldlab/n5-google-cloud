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

import java.net.URI;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Splitter;

public class GoogleCloudStorageURI {
	private static final String storageHost = "storage.googleapis.com";
	private static final String googleCloudHost = "googleapis.com";
	private static final String googleCloudHost2 = "storage.cloud.google.com";

	private static final String storagePathPrefix = "/storage/v1/b/";
	private static final String projectKey = "project";

	private final URI uri;
	private final String bucketName;
	private final String objectKey;
	private final String query;
	private Map<String, String> queryMap;

	public GoogleCloudStorageURI(final String str) {

		this(URI.create(str));
	}

	public GoogleCloudStorageURI(final URI uri) {

		this.uri = uri;
		if (uri.getScheme() == null)
			throw new IllegalArgumentException("Invalid scheme");

		if (uri.getScheme().equalsIgnoreCase("gs")) {
			bucketName = uri.getAuthority();
			objectKey = uri.getPath();
			query = uri.getQuery();
		} else if (uri.getScheme().matches("(?i)http(s)?")) {
			final String host = uri.getHost();
			final String path = uri.getPath();

			final Matcher match;
			if (host.matches("(?i)(www.)?" + googleCloudHost)) {
				match = Pattern.compile("^" + storagePathPrefix + "(?<bucket>[^/]*)(?<key>/?.*)", Pattern.CASE_INSENSITIVE).matcher(path);
			} else if (host.matches("(?i)" + storageHost + "|" + googleCloudHost2)) {
				match = Pattern.compile("/?(?<bucket>[^/]*)(?<key>.*)", Pattern.CASE_INSENSITIVE).matcher(path);
			} else
				match = null;
			if (match == null || !match.matches()) {
				throw new IllegalArgumentException("Not a google cloud storage link");
			}

			bucketName = match.group("bucket");
			objectKey = match.group("key");
			query = uri.getQuery();
		} else {
			throw new IllegalArgumentException("Invalid scheme");
		}
		queryMap = parseQuery();
	}

	public String getBucket() {

		return bucketName;
	}

	public URI asURI() {

		return this.uri;
	}

	public String getKey() {

		return objectKey;
	}

	public String getQuery() {

		return query;
	}

	public String getProject() {

		if (queryMap != null && queryMap.containsKey(projectKey))
			return queryMap.get(projectKey);

		return null;
	}

	private Map<String, String> parseQuery() {

		if (query != null)
			return Splitter.on('&').trimResults().withKeyValueSeparator('=').split(query);
		else
			return null;
	}

}
