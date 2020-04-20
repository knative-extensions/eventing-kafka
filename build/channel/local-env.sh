#!/usr/bin/env bash

# Ensure The Script Is Being Sourced
[[ "${BASH_SOURCE[0]}" == "${0}" ]] && echo -e "\nWARNING: 'Source' this script for the environment setup to apply to the current session!\n"

# Set Local Testing Environment Variables
export SYSTEM_NAMESPACE="knative-eventing"
export CONFIG_LOGGING_NAME="config-logging"
export METRICS_PORT="8081"
export HEALTH_PORT="8082"
export KAFKA_BROKERS="todo"
export KAFKA_USERNAME="todo"
export KAFKA_PASSWORD="todo"

# Log Environment Variables
echo ""
echo "Exported Env Vars..."
echo ""
echo "SYSTEM_NAMESPACE=${SYSTEM_NAMESPACE}"
echo "CONFIG_LOGGING_NAME=${CONFIG_LOGGING_NAME}"
echo "METRICS_PORT=${METRICS_PORT}"
echo "HEALTH_PORT=${HEALTH_PORT}"
echo "KAFKA_BROKERS=${KAFKA_BROKERS}"
echo "KAFKA_USERNAME=${KAFKA_USERNAME}"
echo "KAFKA_PASSWORD=${KAFKA_PASSWORD}"
echo ""
