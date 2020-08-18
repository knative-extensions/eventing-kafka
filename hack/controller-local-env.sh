#!/usr/bin/env bash

# Ensure The Script Is Being Sourced
[[ "${BASH_SOURCE[0]}" == "${0}" ]] && echo -e "\nWARNING: 'Source' this script for the environment setup to apply to the current session!\n"

# Set Local Testing Environment Variables
export SYSTEM_NAMESPACE="knative-eventing"
export SERVICE_ACCOUNT="eventing-kafka-channel-controller"
export METRICS_PORT="8081"
export METRICS_DOMAIN="eventing-kafka"
export KAFKA_PROVIDER="azure"
export KAFKA_BROKERS="eventhub.servicebus.windows.net:9093"
export KAFKA_SECRET=kafka-credentials
export KAFKA_USERNAME="\$ConnectionString"
export KAFKA_PASSWORD="TODO"
export CHANNEL_IMAGE="gcr.io/knative-nightly/eventing-kafka/eventing-kafka-channel:latest"
export DISPATCHER_IMAGE="gcr.io/knative-nightly/eventing-kafka/eventing-kafka-dispatcher:latest"

# Log Environment Variables
echo ""
echo "Exported Env Vars"
echo "-----------------"
echo "SYSTEM_NAMESPACE=${SYSTEM_NAMESPACE}"
echo "SERVICE_ACCOUNT=${SERVICE_ACCOUNT}"
echo "METRICS_PORT=${METRICS_PORT}"
echo "METRICS_DOMAIN=${METRICS_DOMAIN}"
echo "KAFKA_PROVIDER=${KAFKA_PROVIDER}"
echo "KAFKA_BROKERS=${KAFKA_BROKERS}"
echo "KAFKA_SECRET=${KAFKA_SECRET}"
echo "KAFKA_USERNAME=${KAFKA_USERNAME}"
echo "KAFKA_PASSWORD=${KAFKA_PASSWORD}"
echo "CHANNEL_IMAGE=${CHANNEL_IMAGE}"
echo "DISPATCHER_IMAGE=${DISPATCHER_IMAGE}"
echo ""
