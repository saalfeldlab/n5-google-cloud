# N5 Google Cloud
N5 library implementation using Google Cloud Storage backend.

### Implementation specifics
* N5 containers are represented by buckets.
* An `attributes.json` with an empty map is always created for any group. It is used to reliably check group existence as Google Cloud Storage does not have conventional directories.

### OAuth 2.0
This [test](https://github.com/saalfeldlab/n5-google-cloud/blob/master/src/test/java/org/janelia/saalfeldlab/n5/googlecloud/N5GoogleCloudStorageOAuth2Test.java) uses OAuth 2.0 authentication to run unit tests using actual Google Cloud Storage backend. It is excluded from the test run configuration by default and requires a few steps to set up:
1. Create a project in the [Google Cloud console](https://console.cloud.google.com).
1. Go to [APIs & services](https://console.cloud.google.com/apis/credentials) and choose *Create credentials* → *OAuth client ID* → *Other*.
1. Once created, put the provided **client_id** and **client_secret** fields into [src/test/resources/client_secrets.json](https://github.com/saalfeldlab/n5-google-cloud/blob/master/src/test/resources/client_secrets.json).
1. Enable [Storage API](https://console.cloud.google.com/apis/library/storage-component.googleapis.com) and [Resource Manager API](https://console.cloud.google.com/apis/library/cloudresourcemanager.googleapis.com).

This approach allows to obtain temporary security credentials and a refresh token that can be used to obtain new short-term credentials. The [test](https://github.com/saalfeldlab/n5-google-cloud/blob/master/src/test/java/org/janelia/saalfeldlab/n5/googlecloud/N5GoogleCloudStorageOAuth2Test.java) shows how to create a Storage API client in a way that it handles refreshing access tokens internally.
The refresh token remains valid as long as the user has not revoked application access.
