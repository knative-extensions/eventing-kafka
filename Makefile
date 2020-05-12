################################################################################
##
##                     Eventing-Kafka Makefile
##
################################################################################

# Project Directories
BUILD_ROOT:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
BUILD_DIR:=$(BUILD_ROOT)/build
CHANNEL_BUILD_DIR=$(BUILD_DIR)/channel
COMMON_BUILD_DIR=$(BUILD_DIR)/common
CONTROLLER_BUILD_DIR=$(BUILD_DIR)/controller
DISPATCHER_BUILD_DIR=$(BUILD_DIR)/dispatcher

# Flag Controlling Whether To Run Tests Inside Docker Builds Or Not
BUILD_TESTS:=true

# Application Docker Path
# Must be specified manually for image build and push
APP_PATH=gcr.io/knative-nightly/eventing-kafka

# Application Docker Container Version
APP_VERSION=latest

# Application Docker Container Names
CHANNEL_APP_NAME='eventing-kafka-channel'
CONTROLLER_APP_NAME='kafka-channel-controller'
DISPATCHER_APP_NAME='eventing-kafka-dispatcher'

# Application Docker Container Tags
CHANNEL_APP_TAG=$(CHANNEL_APP_NAME):$(APP_VERSION)
CONTROLLER_APP_TAG=$(CONTROLLER_APP_NAME):$(APP_VERSION)
DISPATCHER_APP_TAG=$(DISPATCHER_APP_NAME):$(APP_VERSION)


#
# Clean Build Images
#

clean-channel:
	@echo 'Cleaning Channel Build'
	rm -f $(CHANNEL_BUILD_DIR)/coverage.out $(CHANNEL_BUILD_DIR)/$(CHANNEL_APP_NAME)

clean-common:
	@echo 'Cleaning Common Build'
	rm -f $(COMMON_BUILD_DIR)/coverage.out

clean-controller:
	@echo 'Cleaning Controller Build'
	rm -f $(CONTROLLER_BUILD_DIR)/coverage.out $(DISPATCHER_BUILD_DIR)/$(DISPATCHER_APP_NAME)

clean-dispatcher:
	@echo 'Cleaning Dispatcher Build'
	rm -f $(DISPATCHER_BUILD_DIR)/coverage.out $(DISPATCHER_BUILD_DIR)/$(DISPATCHER_APP_NAME)

clean-all: clean-channel clean-common clean-controller clean-dispatcher

.PHONY: clean-channel clean-common clean-controller clean-dispatcher clean-all


#
# Mod / Fmt / Vet / Lint
#

mod:
	@echo 'Ensuring Dependencies'
	cd $(BUILD_ROOT); go mod tidy; go mod vendor

format:
	@echo 'Formatting Go Source'
	go fmt ./...

vet:
	@echo 'Vetting Packages'
	go vet -v ./cmd/...
	go vet -v ./pkg/...

lint:
	@echo 'Linting Packages'
	golint ./cmd/...
	golint ./pkg/...

.PHONY: mod format vet lint


#
# Testing
#

test-channel:
	@echo 'Testing Channel'
	mkdir -p $(CHANNEL_BUILD_DIR)
	cd $(BUILD_ROOT); go test -v ./pkg/channel/... -coverprofile ${CHANNEL_BUILD_DIR}/coverage.out
	cd $(BUILD_ROOT); go tool cover -func=${CHANNEL_BUILD_DIR}/coverage.out

test-common:
	@echo 'Testing Common'
	mkdir -p $(COMMON_BUILD_DIR)
	cd $(BUILD_ROOT); go test -v ./pkg/common/... -coverprofile ${COMMON_BUILD_DIR}/coverage.out
	cd $(BUILD_ROOT); go tool cover -func=${COMMON_BUILD_DIR}/coverage.out

test-controller:
	@echo 'Testing Controller'
	mkdir -p $(CONTROLLER_BUILD_DIR)
	cd $(BUILD_ROOT); go test -v ./pkg/controller/... -coverprofile ${CONTROLLER_BUILD_DIR}/coverage.out
	cd $(BUILD_ROOT); go tool cover -func=${CONTROLLER_BUILD_DIR}/coverage.out

test-dispatcher:
	@echo 'Testing Dispatcher'
	mkdir -p $(DISPATCHER_BUILD_DIR)
	cd $(BUILD_ROOT); go test -v ./pkg/dispatcher/... -coverprofile ${DISPATCHER_BUILD_DIR}/coverage.out
	cd $(BUILD_ROOT); go tool cover -func=${DISPATCHER_BUILD_DIR}/coverage.out

test-all: test-channel test-common test-controller test-dispatcher

.PHONY: test-channel test-common test-controller test-dispatcher test-all


#
# Build The Native Golang Binaries (Meant For Use In Docker)
#

build-native-channel: clean-channel
	@echo 'Building Channel (Native)'
	mkdir -p $(CHANNEL_BUILD_DIR);
	cd $(BUILD_ROOT)/cmd/channel; go build -o $(CHANNEL_BUILD_DIR)/$(CHANNEL_APP_NAME)

build-native-controller: clean-controller
	@echo 'Building Controller (Native)'
	mkdir -p $(CONTROLLER_BUILD_DIR);
	cd $(BUILD_ROOT)/cmd/controller; go build -o $(CONTROLLER_BUILD_DIR)/$(CONTROLLER_APP_NAME)

build-native-dispatcher: clean-dispatcher
	@echo 'Building Dispatcher (Native)'
	mkdir -p $(DISPATCHER_BUILD_DIR);
	cd $(BUILD_ROOT)/cmd/dispatcher; go build -o $(DISPATCHER_BUILD_DIR)/$(DISPATCHER_APP_NAME)

build-native-all: build-native-channel build-native-controller build-native-dispatcher

.PHONY: build-native-channel build-native-controller build-native-dispatcher build-native-all


#
# Build Docker Containers
#

docker-build-channel:
	@echo 'Building Channel Docker Container'
	docker build --rm -t $(CHANNEL_APP_TAG) --build-arg RUN_TESTS=${BUILD_TESTS} -f $(CHANNEL_BUILD_DIR)/Dockerfile .
	docker tag $(CHANNEL_APP_TAG) $(APP_PATH)/$(CHANNEL_APP_TAG)

docker-build-controller:
	@echo 'Building Controller Docker Container'
	docker build --rm -t $(CONTROLLER_APP_TAG) --build-arg RUN_TESTS=${BUILD_TESTS} -f $(CONTROLLER_BUILD_DIR)/Dockerfile .
	docker tag $(CONTROLLER_APP_TAG) $(APP_PATH)/$(CONTROLLER_APP_TAG)

docker-build-dispatcher:
	@echo 'Building Dispatcher Docker Container'
	docker build --rm -t $(DISPATCHER_APP_TAG) --build-arg RUN_TESTS=${BUILD_TESTS} -f $(DISPATCHER_BUILD_DIR)/Dockerfile .
	docker tag $(DISPATCHER_APP_TAG) $(APP_PATH)/$(DISPATCHER_APP_TAG)

docker-build-all: docker-build-channel docker-build-controller docker-build-dispatcher

.PHONY: docker-build-channel docker-build-controller docker-build-dispatcher docker-build-all


#
# Push Docker Containers
#

docker-push-channel: docker-build-channel
	@echo 'Pushing Channel Docker Container'
	docker push $(APP_PATH)/$(CHANNEL_APP_TAG)

docker-push-controller: docker-build-controller
	@echo 'Pushing Controller Docker Container'
	docker push $(APP_PATH)/$(CONTROLLER_APP_TAG)

docker-push-dispatcher: docker-build-dispatcher
	@echo 'Pushing Dispatcher Docker Container'
	docker push $(APP_PATH)/$(DISPATCHER_APP_TAG)

docker-push-all: docker-push-channel docker-push-controller docker-push-dispatcher

.PHONY: docker-push-channel docker-push-controller docker-push-dispatcher docker-push-all


#
# Support Prow Targets
#

ci-master: docker-push-all

ci-release: docker-push-all

ci-pr: docker-build-all

.PHONY: ci-master ci-release ci-pr
