#!/usr/bin/env bash

# Synchs the release-next branch to master and then triggers CI
# Usage: update-to-head.sh

set -e
REPO_NAME="eventing-kafka"

# Check if there's an upstream release we need to mirror downstream
openshift/release/mirror-upstream-branches.sh

# Reset release-next to upstream/master.
git fetch upstream master
git checkout upstream/master -B release-next

# Update openshift's master and take all needed files from there.
git fetch openshift master
git checkout openshift/master openshift OWNERS Makefile
make generate-dockerfiles
make RELEASE=ci generate-release
git add openshift OWNERS Makefile
git commit -m ":open_file_folder: Update openshift specific files."

# Apply patches if present
PATCHES_DIR="$(pwd)/openshift/patches/"
if [ -d "$PATCHES_DIR" ] && [ "$(ls -A "$PATCHES_DIR")" ]; then
    git apply openshift/patches/*
    make RELEASE=ci generate-release
    git commit -am ":fire: Apply carried patches."
fi
git push -f openshift release-next

# Trigger CI
git checkout release-next -B release-next-ci
date > ci
git add ci
git commit -m ":robot: Triggering CI on branch 'release-next' after synching to upstream/master"
git push -f openshift release-next-ci

if hash hub 2>/dev/null; then
   hub pull-request --no-edit -l "kind/sync-fork-to-upstream" -b openshift-knative/${REPO_NAME}:release-next -h openshift-knative/${REPO_NAME}:release-next-ci
else
   echo "hub (https://github.com/github/hub) is not installed, so you'll need to create a PR manually."
fi
