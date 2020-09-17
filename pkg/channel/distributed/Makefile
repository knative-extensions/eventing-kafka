################################################################################
##
##            Eventing-Kafka Distributed Channel Makefile
##
################################################################################

# Project Directories
BUILD_ROOT:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
BUILD_DIR:=$(BUILD_ROOT)/build
COMMON_BUILD_DIR=$(BUILD_DIR)/common
CONTROLLER_BUILD_DIR=$(BUILD_DIR)/controller
DISPATCHER_BUILD_DIR=$(BUILD_DIR)/dispatcher
RECEIVER_BUILD_DIR=$(BUILD_DIR)/receiver

# TODO - Move to a top level hack script or something?  maybe also a mid level hack script(s) for our tests

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
	go vet -v ../../../cmd/channel/distributed/...
	go vet -v ./...

lint:
	@echo 'Linting Packages'
	golint ../../../cmd/channel/distributed/...
	golint ./...

.PHONY: mod format vet lint


#
# Testing
#

test-common:
	@echo 'Testing Common'
	mkdir -p $(COMMON_BUILD_DIR)
	cd $(BUILD_ROOT); go test -race -v ./common/... -coverprofile ${COMMON_BUILD_DIR}/coverage.out
	cd $(BUILD_ROOT); go tool cover -func=${COMMON_BUILD_DIR}/coverage.out

test-controller:
	@echo 'Testing Controller'
	mkdir -p $(CONTROLLER_BUILD_DIR)
	cd $(BUILD_ROOT); go test -race -v ./controller/... -coverprofile ${CONTROLLER_BUILD_DIR}/coverage.out
	cd $(BUILD_ROOT); go tool cover -func=${CONTROLLER_BUILD_DIR}/coverage.out

test-dispatcher:
	@echo 'Testing Dispatcher'
	mkdir -p $(DISPATCHER_BUILD_DIR)
	cd $(BUILD_ROOT); go test -race -v ./dispatcher/... -coverprofile ${DISPATCHER_BUILD_DIR}/coverage.out
	cd $(BUILD_ROOT); go tool cover -func=${DISPATCHER_BUILD_DIR}/coverage.out

test-receiver:
	@echo 'Testing Receiver'
	mkdir -p $(RECEIVER_BUILD_DIR)
	cd $(BUILD_ROOT); go test -race -v ./receiver/... -coverprofile ${RECEIVER_BUILD_DIR}/coverage.out
	cd $(BUILD_ROOT); go tool cover -func=${RECEIVER_BUILD_DIR}/coverage.out

test-all: test-common test-controller test-dispatcher test-receiver

.PHONY: test-common test-controller test-dispatcher test-receiver test-all
