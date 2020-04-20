# KafkaSecret Reconciler

The files in this directory were NOT generated via 
`knative.dev/pkg/hack/generate-knative.sh` as would normally be expected.  This
is because we're creating a Controller / Reconciler for the K8S CoreV1 Secret
instead of a CRD.  We do not want to generate Controllers / Reconcilers for the
entire CoreV1 set of resources.  Therefore, we've manually copied the previously 
generated code from the `eventing-contrib/kafka'` project and modified it for 
CoreV1 Secrets.  This means that whenever the generation of such code is updated
we should also perform a manual diff and uplift relevant changes here. 
