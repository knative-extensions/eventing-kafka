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


#
# Mod / Fmt / Vet / Lint
#

mod:
	@echo 'Ensuring Dependencies'
	cd $(BUILD_ROOT); go mod vendor; go mod tidy

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
