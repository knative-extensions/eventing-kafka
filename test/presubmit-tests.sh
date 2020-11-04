#!/usr/bin/env bash

# Copyright 2020 The Knative Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script runs the presubmit tests; it is started by prow for each PR.
# For convenience, it can also be executed manually.
# Running the script without parameters, or with the --all-tests
# flag, causes all tests to be executed, in the right order.
# Use the flags --build-tests, --unit-tests and --integration-tests
# to run a specific set of tests.

# If --distributed or --consolidated are used, this is an integration test, and
# we should also pass "--integration-tests"
INTEGRATION_TESTS_FLAG=""

echo "Presubmit-tests command line: $@"

# Parse command-line arguments and remove known ones from the argument array before
# passing it along to the presubmit-tests.sh main function.
for arg do
  shift
  case $arg in
    --distributed)
      export TEST_DISTRIBUTED_CHANNEL=1
      INTEGRATION_TESTS_FLAG="--integration-tests"
      continue
      ;;
    --consolidated)
      export TEST_CONSOLIDATED_CHANNEL=1
      INTEGRATION_TESTS_FLAG="--integration-tests"
      continue
      ;;
  esac
  set -- "$@" "$arg"
done

source $(dirname $0)/../vendor/knative.dev/hack/presubmit-tests.sh

echo "Presubmit-tests environment:"
echo "TEST_DISTRIBUTED_CHANNEL: ${TEST_DISTRIBUTED_CHANNEL}"
echo "TEST_CONSOLIDATED_CHANNEL: ${TEST_CONSOLIDATED_CHANNEL}"
echo "INTEGRATION_TESTS_FLAG: ${INTEGRATION_TESTS_FLAG}"

main "${INTEGRATION_TESTS_FLAG}" $@
