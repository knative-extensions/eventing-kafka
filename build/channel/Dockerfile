FROM golang:1.13.5-buster as builder

# Gather the librdkafka and knative-kafka-dispatcher dependencies for building
RUN wget -qO - https://packages.confluent.io/deb/5.3/archive.key | apt-key add - \
    && echo "deb https://packages.confluent.io/deb/5.3 stable main" >> /etc/apt/sources.list \
    && echo "deb http://security.debian.org/debian-security jessie/updates main" >> /etc/apt/sources.list \
    && apt-get update \
    && apt-get -y install librdkafka-dev

# Copy Over The Project
WORKDIR /go/src/github.com/kyma-incubator/knative-kafka
COPY . .

# Get The RUN_TESTS Argument
ARG RUN_TESTS=false

# Perform The Build Via The Makefile (Optionally Run Tests)
RUN /bin/bash -c "cd /go/src/github.com/kyma-incubator/knative-kafka && \
source ./build/channel/local-env.sh && \
export GOPATH=/go && \
if [[ \"${RUN_TESTS}\" == \"true\" ]]; then \
  make test-channel build-native-channel; \
else \
  make build-native-channel; \
fi"

# Determine Dependencies of knative-kafka-channel that will be needed for the final image and package them into one dir
WORKDIR /channel-deps
RUN ldd /go/src/github.com/kyma-incubator/knative-kafka/build/channel/knative-kafka-channel \
    | tr -s '[:blank:]' '\n' \
    | grep '^/' \
    | xargs -I % sh -c 'mkdir -p $(dirname ./%); cp % ./%;'

# Other dependencies that aren't found using the ldd command. These could be transitive dependencies.
# These were found at runtime generally, when the image reported runtime errors.
RUN cp /lib/x86_64-linux-gnu/ld-linux-x86-64.so.2 ./lib/x86_64-linux-gnu/ld-linux-x86-64.so.2 \
    && cp /lib/x86_64-linux-gnu/libnss_dns.so.2 ./lib/x86_64-linux-gnu/libnss_dns.so.2 \
    && cp /lib/x86_64-linux-gnu/libresolv.so.2 ./lib/x86_64-linux-gnu/libresolv.so.2 \
    && cp /usr/lib/x86_64-linux-gnu/libsasl2.so.2 ./usr/lib/x86_64-linux-gnu/libsasl2.so.2
RUN mkdir -p lib64 \
    && cp /lib64/ld-linux-x86-64.so.2 lib64/ld-linux-x86-64.so.2


# Create The Final Docker Container From Google's Distroless Base
FROM gcr.io/distroless/base

# Manage malloc and os thread count that can execute go code.
ENV MALLOC_ARENA_MAX=1 GOMAXPROCS=1

# Copy over the knative-kafka binary
COPY --from=builder /go/src/github.com/kyma-incubator/knative-kafka/build/channel/knative-kafka-channel /

# Copy over the dependencies
COPY --from=builder /channel-deps/ /

CMD [ "/knative-kafka-channel" ]
