[![Build Status](https://github.com/saalfeldlab/n5-google-cloud/actions/workflows/build-main.yml/badge.svg)](https://github.com/saalfeldlab/n5-google-cloud/actions/workflows/build-main.yml)

# N5 Google Cloud
N5 library implementation using Google Cloud Storage backend.

N5 containers can be represented by either a Google Cloud Storage bucket, or a path (directory) within a bucket.

### Implementation specifics
* In Google Cloud Storage, buckets are created within user projects. If you want to create a bucket using the API, you would need to select one of your projects when creating `Storage`. Although `N5GoogleCloudStorageWriter` supports bucket creation, it's recommended that the bucket already exists before creating an instance of `N5GoogleCloudStorageWriter`.

For reading a bucket or writing into an existing bucket, project id is not required as all buckets have unique names.

### Authentication

Access to non-public buckets requires a few steps to set up the security credentials.

1. Create a project in the [Google Cloud console](https://console.cloud.google.com).
1. Install [Google Cloud SDK](https://cloud.google.com/sdk/docs/).
1. Run `gcloud auth application-default login` to login using OAuth 2.0 and store the credentials. Then, the credentials will be picked up by the code automatically.
