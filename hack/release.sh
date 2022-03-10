#!/usr/bin/env bash

# Copyright 2020 The Knative Authors
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

# Documentation about this script and how to use it can be found
# at https://github.com/knative/test-infra/tree/master/ci

source $(dirname $0)/../vendor/knative.dev/hack/release.sh

export GO111MODULE=on

# Yaml files to generate, and the source config dir for them.
declare -A COMPONENTS
COMPONENTS=(
  ["channel-consolidated.yaml"]="config/channel/consolidated"
  ["channel-distributed.yaml"]="config/channel/distributed"
  ["channel-crds.yaml"]="config/channel/resources"
  ["source.yaml"]="config/source/single"
  ["mt-source.yaml"]="config/source/multi"
  ["source-crds.yaml"]="config/source/common/resources"
  ["source-crd.yaml"]="config/source/common/resources/source"
  ["binding-crd.yaml"]="config/source/common/resources/binding"
)
readonly COMPONENTS

function build_release() {
   # Update release labels if this is a tagged release
  if [[ -n "${TAG}" ]]; then
    echo "Tagged release, updating release labels to kafka.eventing.knative.dev/release: \"${TAG}\""
    LABEL_YAML_CMD=(sed -e "s|kafka.eventing.knative.dev/release: devel|kafka.eventing.knative.dev/release: \"${TAG}\"|")
  else
    echo "Untagged release, will NOT update release labels"
    LABEL_YAML_CMD=(cat)
  fi

  local all_yamls=()
  for yaml in "${!COMPONENTS[@]}"; do
    local config="${COMPONENTS[${yaml}]}"
    echo "Building Knative Eventing Kafka - ${config}"
    ko resolve -j 1 ${KO_FLAGS} -f ${config}/ | "${LABEL_YAML_CMD[@]}" > ${yaml}
    all_yamls+=(${yaml})
  done

  if [ -d "${REPO_ROOT_DIR}/config/channel/post-install" ]; then
    echo "Resolving channel post-install manifests"
    local yaml="channel-post-install.yaml"
    ko resolve -j 1 ${KO_FLAGS} -f config/channel/post-install | "${LABEL_YAML_CMD[@]}" > ${yaml}
    all_yamls+=(${yaml})
  fi

  if [ -d "${REPO_ROOT_DIR}/config/source/post-install" ]; then
    echo "Resolving source post-install manifests"
    local yaml="source-post-install.yaml"
    ko resolve -j 1 ${KO_FLAGS} -f config/source/post-install | "${LABEL_YAML_CMD[@]}" > ${yaml}
    all_yamls+=(${yaml})
  fi

  ARTIFACTS_TO_PUBLISH="${all_yamls[@]}"
}

main $@
