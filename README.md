# n5-google-cloud
N5 library implementation using Google Cloud Storage backend.

## OAuth 2.0
This [test](https://github.com/saalfeldlab/n5-google-cloud/blob/master/src/test/java/org/janelia/saalfeldlab/n5/googlecloud/N5GoogleCloudStorageOAuth2Test.java) uses OAuth 2.0 authentication to run unit tests using actual Google Cloud Storage backend. It is exlcuded from the test run configuration by default and requires a few steps to set up:
1. Create a project in the [Google Cloud console](https://console.cloud.google.com).
1. Go to [APIs & services](https://console.cloud.google.com/apis/credentials) and choose *Create credentials* → *OAuth client ID*.
1. Once created, put the provided **client_id** and **client_secret** fields into [src/test/resources/client_secrets.json](https://github.com/saalfeldlab/n5-google-cloud/blob/master/src/test/resources/client_secrets.json).
1. Enable [Storage API](https://console.cloud.google.com/apis/library/storage-component.googleapis.com) and [Resource Manager API](https://console.cloud.google.com/apis/library/cloudresourcemanager.googleapis.com).
