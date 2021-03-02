#!/bin/bash

branch=${1-'knative-v0.19.1'}
openshift=${2-'4.6'}
promotion_disabled=${3-false}

if [[ "$branch" == "knative-next" ]]; then
    branch="knative-nightly"
fi

cat <<EOF
tag_specification:
  cluster: https://api.ci.openshift.org
  name: '$openshift'
  namespace: ocp
promotion:
  additional_images:
    knative-eventing-kafka-src: src
  disabled: $promotion_disabled
  cluster: https://api.ci.openshift.org
  namespace: openshift
  name: $branch
base_images:
  base:
    name: '$openshift'
    namespace: ocp
    tag: base
build_root:
  project_image:
    dockerfile_path: openshift/ci-operator/build-image/Dockerfile
canonical_go_repository: knative.dev/eventing-kafka
binary_build_commands: make install
test_binary_build_commands: make test-install
tests:
- as: e2e-aws-ocp-${openshift//./}
  steps:
    cluster_profile: aws
    test:
    - as: test
      cli: latest
      commands: make test-e2e
      from: src
      resources:
        requests:
          cpu: 100m
    workflow: ipi-aws
- as: e2e-aws-ocp-${openshift//./}-continuous
  cron: 0 */12 * * 1-5
  steps:
    cluster_profile: aws
    test:
    - as: test
      cli: latest
      commands: make test-e2e
      from: src
      resources:
        requests:
          cpu: 100m
    workflow: ipi-aws
resources:
  '*':
    limits:
      memory: 6Gi
    requests:
      cpu: 4
      memory: 6Gi
  'bin':
    limits:
      memory: 6Gi
    requests:
      cpu: 4
      memory: 6Gi
images:
EOF

core_images=$(find ./openshift/ci-operator/knative-images -mindepth 1 -maxdepth 1 -type d | LC_COLLATE=posix sort)
for img in $core_images; do
  image_base=$(basename $img)
  to_image=$(echo ${image_base//[_.]/-})
  to_image=$(echo ${to_image//v0/upgrade-v0})
  to_image=$(echo ${to_image//migrate/storage-version-migration})
  to_image=$(echo ${to_image//kafka-kafka-/kafka-})
  cat <<EOF
- dockerfile_path: openshift/ci-operator/knative-images/$image_base/Dockerfile
  from: base
  inputs:
    bin:
      paths:
      - destination_dir: .
        source_path: /go/bin/$image_base
  to: knative-eventing-kafka-$to_image
EOF
done

test_images=$(find ./openshift/ci-operator/knative-test-images -mindepth 1 -maxdepth 1 -type d | LC_COLLATE=posix sort)
for img in $test_images; do
  image_base=$(basename $img)
  to_image=$(echo ${image_base//_/-})
  cat <<EOF
- dockerfile_path: openshift/ci-operator/knative-test-images/$image_base/Dockerfile
  from: base
  inputs:
    test-bin:
      paths:
      - destination_dir: .
        source_path: /go/bin/$image_base
  to: knative-eventing-kafka-test-$to_image
EOF
done
