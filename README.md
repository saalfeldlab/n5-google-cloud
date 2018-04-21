# N5 Google Cloud
N5 library implementation using Google Cloud Storage backend.

### Implementation specifics
* N5 containers are represented by buckets.
* In Google Cloud Storage, buckets are created within user projects. Thus, you will need to choose one of your projects in order to create a bucket. For reading a bucket, project id is not required as all buckets have unique names.

### OAuth 2.0
This [test](https://github.com/saalfeldlab/n5-google-cloud/blob/master/src/test/java/org/janelia/saalfeldlab/n5/googlecloud/N5GoogleCloudStorageOAuth2Test.java) uses OAuth 2.0 authentication to run unit tests using actual Google Cloud Storage backend. It is excluded from the test run configuration by default and requires a few steps to set up:
1. Create a project in the [Google Cloud console](https://console.cloud.google.com).
1. Enable [Storage API](https://console.cloud.google.com/apis/library/storage-component.googleapis.com) and [Resource Manager API](https://console.cloud.google.com/apis/library/cloudresourcemanager.googleapis.com).
1. Go to [APIs & services](https://console.cloud.google.com/apis/credentials) and choose *Create credentials* → *OAuth client ID* → *Other*.
1. Run the test. You will be prompted for **client_id** and **client_secret** that are provided on the web page. They are needed to facilitate OAuth 2.0 in order to obtain user credentials. After that, both credentials and client secrets will be stored under `$user.home/.google/n5-google-cloud` and will be automatically loaded from there on all subsequent runs.
