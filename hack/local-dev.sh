#!/usr/bin/env bash

FORCE_BUILD=0
KO_REPOSITORY=ko.local
DOCKER_SOCK=/var/run/docker.sock
DOCKER_BIN=/usr/local/bin/docker
KUBECONFIG=~/.kube/config
KO_CMD=apply

for arg in "$@"
do
    case $arg in
        -f|--forcebuild)
        FORCE_BUILD=1
        shift
        ;;
        --korepo=*)
        KO_REPOSITORY="${arg#*=}"
        shift
        ;;
        --dockersock=*)
        DOCKER_SOCK="${arg#*=}"
        shift
        ;;
        --dockerbin=*)
        DOCKER_BIN="${arg#*=}"
        shift
        ;;
        --kubeconfig=*)
        KUBECONFIG="${arg#*=}"
        shift
        ;;
        --kocmd=*)
        KO_CMD="${arg#*=}"
        shift
        ;;
        --help)
        echo "Script to build docker image and run ko command inside of. Must be executed from root of eventing-kafka project"
        echo
        echo "options:"
        echo "-f, --forcebuild     Force a rebuild of the eventing-kafka-dev docker image"
        echo "--korepo=            Sets KO_DOCKER_REPO environment variable for ko command, default is 'ko.local'"
        echo "--dockersock=        Unix docker socket to mount on docker run of ko build, default is '/var/run/docker.sock'"
        echo "--dockerbin=         Path to docker binary to mount on docker run of ko build, default is '/usr/local/bin/docker'"
        echo "--kubeconfig=        Kubeconfig to be used by ko command, default is '~/.kube/config'"
        echo "--kocmd=             Ko command to be exectued, default is 'apply'"
        exit
        ;;
        *)
        OTHER_ARGUMENTS+=("$1")
        echo "Unknown option specified. Run script with --help for option descriptions"
        exit
        ;;
    esac
done

IMAGE_EXISTS=$(docker images --format "{{.Repository}}:{{.Tag}}" | grep eventing-kafka-dev:latest)
if [[ ! $IMAGE_EXISTS ]] || [[ "$FORCE_BUILD" -eq "1" ]]; then
  docker build -f ./build/local-dev/Dockerfile -t eventing-kafka-dev:latest  .
fi

docker run -it -v /var/run/docker.sock:/var/run/docker.sock \
  -v /usr/local/bin/docker:/usr/local/bin/docker \
  -v ~/.kube/config:/root/.kube/config  \
  -v $(pwd):/go/src/knative.dev/eventing-kafka \
  --env KO_DOCKER_REPO="$KO_REPOSITORY" \
  eventing-kafka-dev

