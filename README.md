# N5 Google Cloud [![Build Status](https://travis-ci.org/saalfeldlab/n5-google-cloud.svg?branch=master)](https://travis-ci.org/saalfeldlab/n5-google-cloud)
N5 library implementation using Google Cloud Storage backend.

### Implementation specifics
* N5 containers are represented by buckets.
* In Google Cloud Storage, buckets are created within user projects. Thus, you will need to choose one of your projects in order to create a bucket. For reading a bucket, project id is not required as all buckets have unique names.

### Authentication

Access to non-public buckets requires a few steps to set up the security credentials.

1. Create a project in the [Google Cloud console](https://console.cloud.google.com).
1. Install [Google Cloud SDK](https://cloud.google.com/sdk/docs/).
1. Run `gcloud auth application-default login` to login using OAuth 2.0 and store the credentials. Then, the credentials will be picked up by the code automatically.
