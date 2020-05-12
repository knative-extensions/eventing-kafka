FROM golang:1.14.2-buster

RUN GO111MODULE=on go get github.com/google/ko/cmd/ko@v0.5.1

ARG KUBECTL_VERSION=v1.17.4
RUN rm -f "$(which kubectl)" && \
    wget "https://storage.googleapis.com/kubernetes-release/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" -O /usr/local/bin/kubectl && \
    chmod +x /usr/local/bin/kubectl

VOLUME ["/go/src/knative.dev/eventing-kafka"]
WORKDIR /go/src/knative.dev/eventing-kafka

ENV KO_DOCKER_REPO=ko.local CGO_ENABLED=1

ENTRYPOINT ko apply -f ./config