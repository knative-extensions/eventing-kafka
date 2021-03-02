#!/usr/bin/env bash

source $(dirname $0)/resolve.sh

release=$1

source_output_file="openshift/release/knative-eventing-kafka-source-ci.yaml"
channel_output_file="openshift/release/knative-eventing-kafka-channel-ci.yaml"
distributed_channel_output_file="openshift/release/knative-eventing-kafka-distributed-channel-ci.yaml"

if [ "$release" == "ci" ]; then
    image_prefix="registry.svc.ci.openshift.org/openshift/knative-nightly:knative-eventing-kafka-"
    tag=""
else
    image_prefix="registry.svc.ci.openshift.org/openshift/knative-${release}:knative-eventing-kafka-"
    tag=""
fi

# the source parts
resolve_resources config/source/single $source_output_file $image_prefix $tag

# the channel parts
resolve_resources config/channel/consolidated $channel_output_file $image_prefix $tag

# the distributed channel parts
resolve_resources config/channel/distributed $distributed_channel_output_file $image_prefix $tag
