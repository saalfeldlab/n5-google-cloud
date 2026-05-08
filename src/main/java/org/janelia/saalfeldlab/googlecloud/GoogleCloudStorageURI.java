package org.janelia.saalfeldlab.googlecloud;

import java.net.URI;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Splitter;
import org.janelia.saalfeldlab.n5.N5URI;

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

		this(N5URI.getAsUri(str));
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
