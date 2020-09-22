# Test
This directory contains tests and testing docs for the knative-sandbox `eventing-kafka` project.

- [Unit Tests](#unit-tests) reside in the codebase alongside the code under test.
- [Integration Tests](#integration-tests) consist of the `conformance` and `e2e` tests also in this directory.
- [Performance Tests](#performance-tests) have yet to be completed but will reside here as well.

While some information has been repeated here, it is useful to be familiar with the other
Knative testing documentation such as...
- [Serving](https://github.com/knative/serving/blob/master/test/README.md)
- [Eventing](https://github.com/knative/eventing/tree/master/test/README.md)
- [Eventing-Contrib](https://github.com/knative/eventing-contrib/blob/master/test/README.md) 


## Unit Tests
The Unit Tests are run by Prow for all Pull Requests.  They are executed by Prow via the [
pre-submit-tests.sh](./presubmit-tests.sh) script with the `--unit-tests` argument. 

The project [Makefile](../Makefile) contains convenience targets for executing the unit-tests
for various portions of the project with a code coverage summary...

```
make test-unit        # All Unit Tests        ( ../pkg/           )
make test-channel     # Channel Unit Tests    ( ../pkg/channel    )
make test-common      # Common Unit Tests     ( ../pkg/common     )
make test-controller  # Controller Unit Tests ( ../pkg/controller )
make test-dispatcher  # Dispatcher Unit Tests ( ../pkg/dispatcher )
```

...or you can always run them manually via the `go test` cmd line...
```
go test -v pkg/channel/...
``` 


## Integration Tests
The `End-To-End` and `Conformance` tests are Knative "integration-tests" meant to be run by Prow for
all Pull Requests.  They are executed by Prow via the [pre-submit-tests.sh](./presubmit-tests.sh)
script with the `--integration-tests` argument.  The existence of the [e2e-tests.sh](./e2e-tests.sh)
script will cause them to be run in that use case.

For local development / maintenance of the tests however, it is convenient to be able to run them 
in various manners which are described here.

>Note - The eventing-kafka project currently has a dependency on the librdkafka C library which
>requires it to be built in a Docker Container matching the target environment.  Therefore, when
>developing locally (on MacOS for instance) you will need to use the [local-dev.sh](../hack/local-dev.sh)
>hack script to start a compliant Docker container / shell when running these commands.


### Pre-Submit Script
The [`presubmit-tests.sh`](./presubmit-tests.sh) script is the entry point for the tests run 
from Prow when a PR is created.

By default, the script will run `build tests`, `unit tests` and `integration tests`.  If you 
only want to run one type of tests, you can run this script with corresponding flags such as...
```
./test/presubmit-tests.sh
./test/presubmit-tests.sh --build-tests
./test/presubmit-tests.sh --unit-tests
./test/presubmit-tests.sh --integration-tests
```
The script will call the [`e2e-tests.sh`](./e2e-tests.sh) script if it exists. 


### E2E-Tests Script
The [`e2e-tests.sh`](./e2e-tests.sh) script is the entry point for running all the e2e tests.

By default, it will create a new GKE cluster in project `$PROJECT_ID`, start Knative Eventing, 
upload test images to `$KO_DOCKER_REPO`, and run the `conformance` and `e2e` tests. After the 
tests finishes, it will delete the cluster.

```
# Complete Tests (Create & Initialize K8S Cluster)
./test/e2e-tests.sh

# Skip Cluster Creation & Install Knative Eventing Into Existing Empty K8S Cluster (kubeconfig)
./test/e2e-tests.sh --run-tests

# Skip Cluster Creation & Knative Eventing Setup - Use Existing K8S Cluster With Eventing Pre-Installed
kubectl apply --filename https://github.com/knative/eventing/releases/download/v0.15.1/eventing.yaml
./test/e2e-tests.sh --run-tests --skip-knative-setup

# Prevent TearDown Of Tests In Order To Debug Failures
./test/e2e-tests.sh --run-tests --skip-teardowns
```
>Remember to provide the `kn-eventing-test-pull-secret` K8S Secret as described in 
>[Private Docker Repositories](#private-docker-repositories) when using an existing cluster.

The script will base the version of Knative Eventing installed in the K8S Cluster on the
current git branch.  If the current git branch is based off of a release branch, it will 
use the latest version of the corresponding Knative Eventing release branch.  If it is 
based off of /master then the latest /master Knative Eventing will be cloned/built/installed.  
Optionally you can set the following environment variable to force a specific Knative Eventing 
release...
```
export KNATIVE_EVENTING_RELEASE_OVERRIDE="v0.15.1"
```


### GO Test Cmd
This is the fastest way to iterate when developing integration tests.  It does require, though,
that you provide the K8S Cluster pre-configured with everything needed to run the tests.  This
includes Knative-Eventing, a Kafka Cluster, and the KafkaChannel CR implementation.  Your
current `kubeconfig` should refer to the cluster.  Further, you are responsible for providing
the Knative Eventing [Test Images](#test-images) which are normally built for you when running
the [e2e-tests.sh](./e2e-tests.sh) script.

Unlike the test scripts described above you don't need to run these tests from within the custom
Docker container / shell setup by [local-dev.sh](../hack/local-dev.sh), but can run them directly
from the root of the project.

```
# Run All The Conformance Tests Against The KafkaChannel
go test -v -tags=e2e -count=1 ./test/conformance/... -channels=messaging.knative.dev/v1beta1:KafkaChannel

# Run All The E2E Tests Against The KafkaChannel
go test -v -tags=e2e -count=1 ./test/e2e/... -channels=messaging.knative.dev/v1beta1:KafkaChannel

# Run Only The 'TestChannelChain' E2E Test Against The KafkaChannel
go test -v -tags=e2e -count=1 ./test/e2e/... -channels=messaging.knative.dev/v1beta1:KafkaChannel -run TestChannelChain
```
> Note - The `-tags=e2e` argument is required to run the integration tests with `go test`! 


## Performance Tests
> TODO - Implement Performance Tests ; ) 


## Test Images
The integration tests primarily delegate to the Knative Eventing generic test implementations.
These implementations use various [test_images](../vendor/knative.dev/eventing/test/test_images)
in order to verify the desired behavior.  Therefore, these `test_images` need to have been built
and be available for downloading in the cluster.  The [e2e-tests.sh](./e2e-tests.sh) script will
build and publish these for you, but you are responsible for providing them when running the
tests directly via `go test` commands.

To manually build and publish the test_images to `$KO_DOCKER_REPO` from the project root...
```
export KO_DOCKER_REPO=<my-docker-repo>
export VENDOR_EVENTING_TEST_IMAGES="vendor/knative.dev/eventing/test/test_images/"
sed -i 's@knative.dev/eventing/test/test_images@knative.dev/eventing-kafka/vendor/knative.dev/eventing/test/test_images@g' "${VENDOR_EVENTING_TEST_IMAGES}"*/*.yaml
./test/upload-test-images.sh vendor/knative.dev/eventing/test/test_images/ e2e
sed -i 's@knative.dev/eventing-kafka/vendor/knative.dev/eventing/test/test_images@knative.dev/eventing/test/test_images@g' "${VENDOR_EVENTING_TEST_IMAGES}"*/*.yaml
``` 
>Note - Set `$KO_DOCKER_REPO` to whatever Docker Repository you intend to use.  You cannot use
>`ko.local` for the Knative Eventing `test_images` because they will be deployed with an 
>`ImagePullPolicy` of `Always` which will bypass the local docker repo.  See 
>[Private Docker Repositories](#private-docker-repositories) for further information.

>Note - See [MacOS Utils](#macos-utils) when running on Mac and replace `sed` in above commands
>with `gsed`.

New test images should be placed in `./test/test_images`. For each image create a new sub-folder 
and include a Go file that will be an entry point to the application. This Go file should use 
the package `main` and include the function `main()`. It is a good practice to include a `README`
file as well. When uploading test images, `ko` will build an image from this folder.


## Private Docker Repositories
If you are using a private Docker repository, then you will have to ensure that you are locally
authenticated, and that your local docker is configured to use that repository.

You will need to set the `$KO_DOCKER_REPO` environment variable to refer to your private Docker Repo, and
to pass that value as `--korepo=$KO_DOCKER_REPO` when using the [local-dev.sh](../hack/local-dev.sh) script.
The script and Docker container currently only supports GCR authentication, but others can be added as needed.

The K8S cluster used in the integration and performance tests will need to have proper credentials in order
to pull the `test_images` and other artifacts.  As discussed in the Knative Eventing 
[documentation](https://github.com/knative/eventing/tree/master/test#running-end-to-end-tests)
you can provide a K8S Secret with `type: kubernetes.io/dockerconfigjson` named `kn-eventing-test-pull-secret`
in the `default` namespace for the scripts/tests to use. 


## MacOS Utils
The [e2e-tests.sh](./e2e-tests.sh) script relies on a couple GNU utilities that are not native on MacOS (Darwin).  
These can be installed as follows... 
```
brew install grep
brew install gnu-sed
```
...and are available as `ggrep` and `gsed` respectively. 
