#!/usr/bin/env bash

IMAGE_EXISTS=$(docker images --format "{{.Repository}}:{{.Tag}}" | grep eventing-kafka-dev:latest)
if [[ ! $IMAGE_EXISTS ]]; then
  docker build -f ./build/local-dev/Dockerfile -t eventing-kafka-dev:latest  .
fi

docker run -it -v /var/run/docker.sock:/var/run/docker.sock \
  -v /usr/local/bin/docker:/usr/local/bin/docker \
  -v ~/.kube/config:/root/.kube/config  \
  -v $(pwd):/go/src/knative.dev/eventing-kafka \
  eventing-kafka-dev

