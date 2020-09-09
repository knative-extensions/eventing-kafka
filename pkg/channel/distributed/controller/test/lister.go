package test

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakekubeclientset "k8s.io/client-go/kubernetes/fake"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	kafkav1beta1 "knative.dev/eventing-contrib/kafka/channel/pkg/apis/messaging/v1beta1"
	fakekafkaclientset "knative.dev/eventing-contrib/kafka/channel/pkg/client/clientset/versioned/fake"
	kafkalisters "knative.dev/eventing-contrib/kafka/channel/pkg/client/listers/messaging/v1beta1"
	fakeeventingclientset "knative.dev/eventing/pkg/client/clientset/versioned/fake"
	fakeeventsclientset "knative.dev/eventing/pkg/client/clientset/versioned/fake"
	"knative.dev/pkg/reconciler/testing"
)

var clientSetSchemes = []func(*runtime.Scheme) error{
	fakekubeclientset.AddToScheme,
	fakeeventsclientset.AddToScheme,
	fakekafkaclientset.AddToScheme,
	fakeeventingclientset.AddToScheme,
}

type Listers struct {
	sorter testing.ObjectSorter
}

func NewListers(objs []runtime.Object) Listers {

	scheme := runtime.NewScheme()

	for _, addTo := range clientSetSchemes {
		addTo(scheme)
	}

	ls := Listers{
		sorter: testing.NewObjectSorter(scheme),
	}

	ls.sorter.AddObjects(objs...)

	return ls
}

func (l *Listers) indexerFor(obj runtime.Object) cache.Indexer {
	return l.sorter.IndexerForObjectType(obj)
}

func (l *Listers) GetKubeObjects() []runtime.Object {
	return l.sorter.ObjectsForSchemeFunc(fakekubeclientset.AddToScheme)
}

func (l *Listers) GetEventingObjects() []runtime.Object {
	return l.sorter.ObjectsForSchemeFunc(fakeeventingclientset.AddToScheme)
}

func (l *Listers) GetEventsObjects() []runtime.Object {
	return l.sorter.ObjectsForSchemeFunc(fakeeventsclientset.AddToScheme)
}

func (l *Listers) GetKafkaChannelObjects() []runtime.Object {
	return l.sorter.ObjectsForSchemeFunc(fakekafkaclientset.AddToScheme)
}

func (l *Listers) GetAllObjects() []runtime.Object {
	all := l.GetKafkaChannelObjects()
	all = append(all, l.GetEventsObjects()...)
	all = append(all, l.GetKubeObjects()...)
	return all
}

func (l *Listers) GetSecretLister() corev1listers.SecretLister {
	return corev1listers.NewSecretLister(l.indexerFor(&corev1.Secret{}))
}

func (l *Listers) GetServiceLister() corev1listers.ServiceLister {
	return corev1listers.NewServiceLister(l.indexerFor(&corev1.Service{}))
}

func (l *Listers) GetEndpointsLister() corev1listers.EndpointsLister {
	return corev1listers.NewEndpointsLister(l.indexerFor(&corev1.Endpoints{}))
}

func (l *Listers) GetKafkaChannelLister() kafkalisters.KafkaChannelLister {
	return kafkalisters.NewKafkaChannelLister(l.indexerFor(&kafkav1beta1.KafkaChannel{}))
}

func (l *Listers) GetDeploymentLister() appsv1listers.DeploymentLister {
	return appsv1listers.NewDeploymentLister(l.indexerFor(&appsv1.Deployment{}))
}
