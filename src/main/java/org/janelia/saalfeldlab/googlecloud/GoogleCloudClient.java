package org.janelia.saalfeldlab.googlecloud;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.auth.Credentials;

public abstract class GoogleCloudClient<T> {

	public static interface Scope {

		@Override
		public String toString();

		public static Collection<String> toScopeStrings(final Collection<? extends Scope> scopes) {

			final List<String> scopeStrings = new ArrayList<>();
			for (final Scope scope : scopes)
				scopeStrings.add(scope.toString());
			return scopeStrings;
		}
	}

	protected final Credentials credentials;

	public GoogleCloudClient(final Credentials credentials) {

		this.credentials = credentials;
	}

	public abstract T create();
}
