#!/bin/bash
# A script that will update the mapping file in github.com/openshift/release

set -e
readonly TMPDIR=$(mktemp -d knativeEventingPeriodicReporterXXXX -p /tmp/)
fail() { echo; echo "$*"; exit 1; }

cat >> "$TMPDIR"/reporterConfig <<EOF
  reporter_config:
    slack:
      channel: '#knative-eventing'
      job_states_to_report:
      - success
      - failure
      - error
      report_template: '{{if eq .Status.State "success"}} :rainbow: Job *{{.Spec.Job}}* ended with *{{.Status.State}}*. <{{.Status.URL}}|View logs> :rainbow: {{else}} :volcano: Job *{{.Spec.Job}}* ended with *{{.Status.State}}*. <{{.Status.URL}}|View logs> :volcano: {{end}}'
EOF


# Deduce X.Y.Z version from branch name
BRANCH=$(git rev-parse --abbrev-ref HEAD)
VERSION=$(echo $BRANCH | sed -E 's/^.*(v[0-9]+\.[0-9]+\.[0-9]+|next)|.*/\1/')
test -n "$VERSION" || fail "'$BRANCH' is not a release branch"
VER=$(echo $VERSION | sed 's/\./_/;s/\.[0-9]\+$//') # X_Y form of version


# Set up variables for important locations in the openshift/release repo.
OPENSHIFT=$(realpath "$1"); shift
test -d "$OPENSHIFT/.git" || fail "'$OPENSHIFT' is not a git repo"
MIRROR="$OPENSHIFT/core-services/image-mirroring/knative/mapping_knative_${VER}_quay"
CONFIGDIR=$OPENSHIFT/ci-operator/config/openshift-knative/eventing-kafka
test -d "$CONFIGDIR" || fail "'$CONFIGDIR' is not a directory"
PERIODIC_CONFIGDIR=$OPENSHIFT/ci-operator/jobs/openshift-knative/eventing-kafka
test -d "$PERIODIC_CONFIGDIR" || fail "'$PERIODIC_CONFIGDIR' is not a directory"

# Generate CI config files
CONFIG=$CONFIGDIR/openshift-knative-eventing-kafka-release-$VERSION
PERIODIC_CONFIG=$PERIODIC_CONFIGDIR/openshift-knative-eventing-kafka-release-$VERSION-periodics.yaml
CURDIR=$(dirname $0)
$CURDIR/generate-ci-config.sh knative-$VERSION 4.6 > ${CONFIG}__46.yaml
$CURDIR/generate-ci-config.sh knative-$VERSION 4.7 true > ${CONFIG}__47.yaml

# Switch to openshift/release to generate PROW files
cd $OPENSHIFT
echo "Generating PROW files in $OPENSHIFT"
make jobs
make ci-operator-config
# We have to do this manually, see: https://docs.ci.openshift.org/docs/how-tos/notification/
echon "==== Adding reporter_config to periodics ===="
# These version MUST match the ocp version we used above
for OCP_VERSION in 46 47; do
    sed -i "/  name: periodic-ci-openshift-knative-eventing-kafka-release-${VERSION}-${OCP_VERSION}-e2e-aws-ocp-${OCP_VERSION}-continuous\n  spec:/ r $TMPDIR/reporterConfig" "$PERIODIC_CONFIG"
done
echo "==== Changes made to $OPENSHIFT ===="
git status
echo "==== Commit changes to $OPENSHIFT and create a PR"
