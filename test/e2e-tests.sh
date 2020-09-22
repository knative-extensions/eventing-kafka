#!/usr/bin/env bash

# Copyright 2019 The Knative Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script runs the end-to-end tests against eventing-kafka built
# from source.

# If you already have the *_OVERRIDE environment variables set, call
# this script with the --run-tests arguments and it will use the cluster
# and run the tests.
# Note that local clusters often do not have the resources to run 12 parallel
# tests (the default) as the tests each tend to create their own namespaces and
# dispatchers.  For example, a local Docker cluster with 4 CPUs and 8 GB RAM will
# probably be able to handle 6 at maximum.  Be sure to adequately set the
# MAX_PARALLEL_TESTS variable before running this script.

# Calling this script without any arguments will create a new cluster in
# project $PROJECT_ID, start Knative eventing system, install resources
# in eventing-kafka, run the tests and delete the cluster.

# Variables supported by this script:
# PROJECT_ID:  the GKR project in which to create the new cluster (unless using "--run-tests")
# MAX_PARALLEL_TESTS:  The maximum number of go tests to run in parallel (via "-test.parallel", default 12)

TEST_PARALLEL=${MAX_PARALLEL_TESTS:-12}

# Include the test-infra e2e script functions (See knative.dev/test-infra/scripts/README.md)
source $(dirname $0)/../vendor/knative.dev/test-infra/scripts/e2e-tests.sh

# If gcloud is not available make it a no-op, not an error.
which gcloud &> /dev/null || gcloud() { echo "[ignore-gcloud $*]" 1>&2; }

# Use GNU Tools on MacOS (Requires the 'grep' and 'gnu-sed' Homebrew formulae)
if [ "$(uname)" == "Darwin" ]; then
  sed=gsed
  grep=ggrep
fi

# Eventing main config path from HEAD.
readonly EVENTING_CONFIG="./config/"
readonly EVENTING_MT_CHANNEL_BROKER_CONFIG="./config/brokers/mt-channel-broker"
readonly EVENTING_IN_MEMORY_CHANNEL_CONFIG="./config/channels/in-memory-channel"

# Config tracing config.
readonly CONFIG_TRACING_CONFIG="test/config/config-tracing.yaml"

# Strimzi Operator / Cluster Installation Namespace
readonly STRIMZI_KAFKA_NAMESPACE="kafka" # Installation Namespace

# Strimzi Cluster Operator Installation Template
readonly STRIMZI_CLUSTER_OPERATOR_CONFIG="test/config/100-strimzi-cluster-operator-0.18.0.yaml"

# Strimzi Kafka Cluster Instance CR
readonly STRIMZI_KAFKA_CLUSTER_CONFIG="test/config/200-kafka-ephemeral-single-0.18.0.yaml"

# Strimzi Kafka Cluster Brokers URL (base64 encoded value for k8s secret)
readonly STRIMZI_KAFKA_CLUSTER_BROKERS="my-cluster-kafka-bootstrap.kafka.svc:9092"
readonly STRIMZI_KAFKA_CLUSTER_BROKERS_ENCODED="bXktY2x1c3Rlci1rYWZrYS1ib290c3RyYXAua2Fma2Euc3ZjOjkwOTI=" # Simple base64 encoding of STRIMZI_KAFKA_CLUSTER_BROKERS

# Eventing Kafka main config path from HEAD.
readonly EVENTING_KAFKA_CONFIG_DIR="./config/channel/distributed"

# Eventing Kafka Channel CRD Config Temp Dir (Used to modify config yaml)
readonly EVENTING_KAFKA_CONFIG_TEMP_DIR="$(mktemp -d)"

# Eventing Kafka Channel CRD Secret (Will be modified to with Strimzi Auth)
readonly EVENTING_KAFKA_SECRET_TEMPLATE="300-kafka-secret.yaml"

# Vendored Eventing Test Images
readonly VENDOR_EVENTING_TEST_IMAGES="vendor/knative.dev/eventing/test/test_images/"


# Utility Function For Generating Previous Knative Eventing Release URLs
function kn_eventing_previous_release_url() {
    local version=$1
    echo "https://storage.googleapis.com/knative-releases/eventing/previous/${version}/eventing.yaml"
}

#
# TODO - Consider adding this function to the test-infra library.sh utilities ?
#
# Add The kn-eventing-test-pull-secret To Specified ServiceAccount & Restart Pods
#
# If the default namespace contains a Secret named 'kn-eventing-test-pull-secret',
# then copy it into the specified Namespace and add it to the specified ServiceAccount,
# and restart the specified Pods.
#
# This utility function exists to support local cluster testing with a private Docker
# repository, and is based on the CopySecret() functionality in eventing/pkg/utils.
#
function add_kn_eventing_test_pull_secret() {

  # Local Constants
  local secret="kn-eventing-test-pull-secret"

  # Get The Function Arguments
  local namespace="$1"
  local account="$2"
  local deployment="$3"

  # If The Eventing Test Pull Secret Is Present & The Namespace Was Specified
  if [[ $(kubectl get secret $secret -n default --ignore-not-found --no-headers=true | wc -l) -eq 1 && -n "$namespace" ]]; then

      # If The Secret Is Not Already In The Specified Namespace Then Copy It In
      if [[ $(kubectl get secret $secret -n "$namespace" --ignore-not-found --no-headers=true | wc -l) -lt 1 ]]; then
        kubectl get secret $secret -n default -o yaml | sed "s/namespace: default/namespace: $namespace/" | kubectl create -f -
      fi

      # If Specified Then Patch The ServiceAccount To Include The Image Pull Secret
      if [[ -n "$account" ]]; then
        kubectl patch serviceaccount -n "$namespace" "$account" -p "{\"imagePullSecrets\": [{\"name\": \"$secret\"}]}"
      fi

      # If Specified Then Restart The Pods Of The Deployment
      if [[ -n "$deployment" ]]; then
        kubectl rollout restart -n "$namespace" deployment "$deployment"
      fi
  fi
}

# E2E Knative Setup Lifecycle Function (Defaults To Knative Eventing HEAD)
function knative_setup() {

  # Install Knative Eventing (Override, Release Branch Or Latest)
  if [[ -n "${KNATIVE_EVENTING_RELEASE_OVERRIDE}" ]]; then
    echo ">> Install Knative Eventing from override ${KNATIVE_EVENTING_RELEASE_OVERRIDE}"
    kubectl apply -f "$(kn_eventing_previous_release_url ${KNATIVE_EVENTING_RELEASE_OVERRIDE})"
  elif is_release_branch; then
    echo ">> Install Knative Eventing from ${KNATIVE_EVENTING_RELEASE}"
    kubectl apply -f ${KNATIVE_EVENTING_RELEASE}
  else
    echo ">> Install Knative Eventing from HEAD"
    pushd .
    cd ${GOPATH} && mkdir -p src/knative.dev && cd src/knative.dev
    git clone https://github.com/knative/eventing
    cd eventing
    ko apply -f ${EVENTING_CONFIG}
    # Install MT Channel Based Broker
    ko apply -f ${EVENTING_MT_CHANNEL_BROKER_CONFIG}
    popd
  fi

  # Add The kn-eventing-test-pull-secret (If Present) To ServiceAccounts & Restart Deployments
  for name in eventing-controller eventing-webhook mt-broker-filter mt-broker-ingress pingsource-mt-adapter; do
    add_kn_eventing_test_pull_secret knative-eventing ${name} ${name}
  done

  # Wait For All The Knative Eventing Pods To Start
  wait_until_pods_running knative-eventing || fail_test "Knative Eventing did not come up"
}

# E2E Knative Teardown Lifecycle Function (Defaults To Knative Eventing HEAD)
function knative_teardown() {
  # Uninstall Knative Eventing (Override, Release Branch Or Latest)
  if [[ -n "${KNATIVE_EVENTING_RELEASE_OVERRIDE}" ]]; then
    echo ">> Uninstalling Knative Eventing from override ${KNATIVE_EVENTING_RELEASE_OVERRIDE}"
    kubectl delete -f "$(kn_eventing_previous_release_url ${KNATIVE_EVENTING_RELEASE_OVERRIDE})"
  elif is_release_branch; then
    echo ">> Uninstalling Knative Eventing from release ${KNATIVE_EVENTING_RELEASE}"
    kubectl delete -f "${KNATIVE_EVENTING_RELEASE}"
  else
    echo ">> Uninstalling Knative Eventing from HEAD"
    pushd .
    cd ${GOPATH}/src/knative.dev/eventing
    ko delete --ignore-not-found=true --now --timeout 60s -f ./config/brokers/channel-broker
    ko delete --ignore-not-found=true --now --timeout 60s -f ./config/
    popd
  fi

  # Wait For All The Knative Eventing Namespace To Be Removed
  wait_until_object_does_not_exist namespaces knative-eventing
}

# Install Strimzi Kafka Operator & Cluster Into K8S Cluster
function kafka_setup() {

  # Create The Namespace Where Strimzi Kafka Will Be Installed
  echo "Installing Kafka Cluster"
  kubectl create namespace ${STRIMZI_KAFKA_NAMESPACE} || return 1

  # Install Strimzi Into The Desired Namespace (Dynamically Changing The Namespace)
  sed "s/namespace: .*/namespace: ${STRIMZI_KAFKA_NAMESPACE}/" ${STRIMZI_CLUSTER_OPERATOR_CONFIG} | kubectl apply -f - -n ${STRIMZI_KAFKA_NAMESPACE}

  # Wait For The Strimzi Kafka Cluster Operator To Be Ready (Forcing Delay To Ensure CRDs Are Installed To Prevent Race Condition)
  wait_until_pods_running ${STRIMZI_KAFKA_NAMESPACE} || fail_test "Failed to start up Strimzi Cluster Operator"

  # Create The Actual Kafka Cluster Instance For The Cluster Operator To Setup
  kubectl apply -f ${STRIMZI_KAFKA_CLUSTER_CONFIG} -n ${STRIMZI_KAFKA_NAMESPACE}

  # Delay Pod Running Check Until All Pods Are Created To Prevent Race Condition (Strimzi Kafka Instance Can Take A Bit To Spin Up)
  local iterations=0
  local progress="Waiting for Kafka Pods to be created..."
  while [[ $(kubectl get pods --no-headers=true -n ${STRIMZI_KAFKA_NAMESPACE} | wc -l) -lt 6 && $iterations -lt 60 ]] # 1 ClusterOperator, 3 Zookeeper, 1 Kafka, 1 EntityOperator
  do
    echo -ne "${progress}\r"
    progress="${progress}."
    iterations=$((iterations + 1))
    sleep 3
  done
  echo "${progress}"

  # Wait For The Strimzi Kafka Cluster To Be Ready
  wait_until_pods_running ${STRIMZI_KAFKA_NAMESPACE} || fail_test "Failed to start up a Strimzi Kafka Instance"
}

# Uninstall Strimzi Kafka Operator & Cluster From K8S Cluster
function kafka_teardown() {
  echo "Uninstalling Kafka Cluster"
  kubectl delete -f ${STRIMZI_KAFKA_CLUSTER_CONFIG} -n ${STRIMZI_KAFKA_NAMESPACE}
  kubectl delete -f ${STRIMZI_CLUSTER_OPERATOR_CONFIG} -n ${STRIMZI_KAFKA_NAMESPACE}
  kubectl delete namespace ${STRIMZI_KAFKA_NAMESPACE}
}

# Install The eventing-kafka KafkaChannel Implementation Via Ko
function channel_setup() {

  # Copy The eventing-kafka Config To Temp Directory For Modification
  echo "Installing KafkaChannel"
  cp ${EVENTING_KAFKA_CONFIG_DIR}/*yaml "${EVENTING_KAFKA_CONFIG_TEMP_DIR}"

  # Update The Kafka Secret With Strimzi Kafka Cluster Brokers (No Authentication)
  sed -i "s/brokers: RU1QVFk=/brokers: ${STRIMZI_KAFKA_CLUSTER_BROKERS_ENCODED}/" "${EVENTING_KAFKA_CONFIG_TEMP_DIR}/${EVENTING_KAFKA_SECRET_TEMPLATE}"
  sed -i "s/namespace: RU1QVFk=/namespace: \"\"/" "${EVENTING_KAFKA_CONFIG_TEMP_DIR}/${EVENTING_KAFKA_SECRET_TEMPLATE}"
  sed -i "s/password: RU1QVFk=/password: \"\"/" "${EVENTING_KAFKA_CONFIG_TEMP_DIR}/${EVENTING_KAFKA_SECRET_TEMPLATE}"
  sed -i "s/username: RU1QVFk=/username: \"\"/" "${EVENTING_KAFKA_CONFIG_TEMP_DIR}/${EVENTING_KAFKA_SECRET_TEMPLATE}"

  # Install The eventing-kafka KafkaChannel Implementation
  ko apply -f "${EVENTING_KAFKA_CONFIG_TEMP_DIR}" || return 1

  # Add The kn-eventing-test-pull-secret (If Present) To ServiceAccount & Restart eventing-kafka Deployment
  add_kn_eventing_test_pull_secret knative-eventing eventing-kafka-channel-controller eventing-kafka-channel-controller

  # Wait Until All The eventing-kafka Pods Are Running
  wait_until_pods_running knative-eventing || fail_test "Failed to install the Kafka Channel CRD"
}

# Uninstall The eventing-kafka KafkaChannel Implementation Via Ko
function channel_teardown() {
  echo "Uninstalling KafkaChannel"
  kubectl delete secret -n knative-eventing kafka-cluster
  sleep 10 # Give Controller Time To React To Kafka Secret Deletion ; )
  ko delete --ignore-not-found=true --now --timeout 60s -f "${EVENTING_KAFKA_CONFIG_TEMP_DIR}"
}

# E2E Test Setup Lifecycle Function
function test_setup() {

  # Setup Kafka & eventing-kafka Channel
  kafka_setup || return 1
  channel_setup || return 1

  # Install kail If Necessary
  if ! which kail > /dev/null; then
    bash <( curl -sfL https://raw.githubusercontent.com/boz/kail/master/godownloader.sh) -b "$GOPATH/bin"
  fi

  # Capture All Logs With Kail
  kail > ${ARTIFACTS}/k8s.log.txt &
  local kail_pid=$!

  # Clean Up Kail To Prevent Interference With Shut-Down
  add_trap "kill $kail_pid || true" EXIT

  # Publish Test Images (Adjust/Revert Ko Paths For Current GOPATH Location In Vendored Eventing Code)
  echo ">> Publishing test images from eventing"
  sed -i 's@knative.dev/eventing/test/test_images@knative.dev/eventing-kafka/vendor/knative.dev/eventing/test/test_images@g' "${VENDOR_EVENTING_TEST_IMAGES}"*/*.yaml
  $(dirname $0)/upload-test-images.sh ${VENDOR_EVENTING_TEST_IMAGES} e2e || fail_test "Error uploading eventing test images"
  sed -i 's@knative.dev/eventing-kafka/vendor/knative.dev/eventing/test/test_images@knative.dev/eventing/test/test_images@g' "${VENDOR_EVENTING_TEST_IMAGES}"*/*.yaml

  # This upload-test-images.sh command will fail if the test/test_images directory does not exist; uncomment when we have eventing-kafka specific images here
  # $(dirname $0)/upload-test-images.sh "test/test_images" e2e || fail_test "Error uploading local test images"
}

# E2E Test Teardown Lifecycle Function
function test_teardown() {

  # Teardown Kafka & eventing-kafka Channel
  kafka_teardown
  channel_teardown
}

if [[ "$@" =~ "--run-tests" ]]; then
  # If the local test-created clusterrolebinding is still in place, it needs to be removed or the initialize call will fail.
  # See https://github.com/knative/test-infra/issues/2446
  kubectl delete clusterrolebindings.rbac.authorization.k8s.io "cluster-admin-binding-${USER}" 2> /dev/null
elif [[ ! -x "$(command -v kubetest2)" ]]; then
  # If creating a remote cluster, the "kubetest2" utility must be installed.
  # If it is not already available we attempt to install it here
  GO111MODULE=on go get sigs.k8s.io/kubetest2/kubetest2-gke@latest
  GO111MODULE=on go get sigs.k8s.io/kubetest2/kubetest2-tester-exec@latest
fi

# Initialize The Test Lifecycle (Without Istio)
# Note:  The setting of gcp-project-id option here has no effect when testing locally; it is only for the kubetest2 utility
# If you wish to use this script just as test setup, *without* teardown, add "--skip-teardowns" to the initialize command
initialize $@ --skip-istio-addon "--gcp-project-id=${PROJECT_ID}"

# Run The Conformance Tests
go_test_e2e -timeout=5m "-test.parallel=${TEST_PARALLEL}" ./test/conformance -channels=messaging.knative.dev/v1beta1:KafkaChannel || fail_test

# Run The E2E Tests
go_test_e2e -timeout=20m "-test.parallel=${TEST_PARALLEL}" ./test/e2e -channels=messaging.knative.dev/v1beta1:KafkaChannel || fail_test

# Teardown Test Lifecycle With Success Status
success
