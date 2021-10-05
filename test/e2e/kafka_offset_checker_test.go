//+build e2e

package e2e

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
	testlib "knative.dev/eventing/test/lib"
	pkgtest "knative.dev/pkg/test"
	"knative.dev/reconciler-test/pkg/k8s"

	"knative.dev/eventing-kafka/test/rekt/features/kafkasource"
)

func TestInitOffset(t *testing.T) {

	ctx := context.Background()

	// Run Test In Parallel With Others
	client := testlib.Setup(t, true)
	defer testlib.TearDown(client)

	jobName := "kafka-offset-checker"
	imageName := "kafka-offset-checker"

	j := kafkaOffsetsCheckerJob(jobName, imageName)
	j.Namespace = client.Namespace
	_, err := client.Kube.BatchV1().Jobs(client.Namespace).Create(ctx, j, metav1.CreateOptions{})
	if err != nil {
		t.Fatal("Failed to create job", err)
	}

	if err := k8s.WaitUntilJobDone(ctx, client.Kube, client.Namespace, jobName); err != nil {
		t.Errorf("Failed waiting for job %s to be completed: %v", jobName, err)
	}
}

func kafkaOffsetsCheckerJob(jobName string, imageName string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: jobName,
		},
		Spec: batchv1.JobSpec{
			Completions:  pointer.Int32Ptr(5),
			BackoffLimit: pointer.Int32Ptr(1),
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name:            jobName,
					Image:           pkgtest.ImagePath(imageName),
					ImagePullPolicy: corev1.PullIfNotPresent,
					Env: []corev1.EnvVar{
						{Name: "SYSTEM_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
						{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
						{Name: "KAFKA_SERVERS", Value: kafkasource.KafkaBootstrapUrlPlain},
					},
				}},
				RestartPolicy: corev1.RestartPolicyNever,
			}},
		}}
}
