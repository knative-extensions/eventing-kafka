package testing

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakekubeclientset "k8s.io/client-go/kubernetes/fake"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	fakemessagingclientset "knative.dev/eventing-kafka/pkg/client/clientset/versioned/fake"
	versionedscheme "knative.dev/eventing-kafka/pkg/client/clientset/versioned/scheme"
	messaginglisters "knative.dev/eventing-kafka/pkg/client/listers/messaging/v1beta1"
	"knative.dev/pkg/reconciler/testing"
)

var clientSetSchemes = []func(*runtime.Scheme) error{
	fakekubeclientset.AddToScheme,
	fakemessagingclientset.AddToScheme,
	versionedscheme.AddToScheme,
}

type Listers struct {
	sorter testing.ObjectSorter
}

func NewListers(objs []runtime.Object) Listers {
	scheme := runtime.NewScheme()

	for _, addTo := range clientSetSchemes {
		_ = addTo(scheme)
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

func (l *Listers) GetMessagingObjects() []runtime.Object {
	return l.sorter.ObjectsForSchemeFunc(fakemessagingclientset.AddToScheme)
}

func (l *Listers) GetAllObjects() []runtime.Object {
	all := l.GetMessagingObjects()
	all = append(all, l.GetKubeObjects()...)
	return all
}

func (l *Listers) GetServiceLister() corev1listers.ServiceLister {
	return corev1listers.NewServiceLister(l.indexerFor(&corev1.Service{}))
}

func (l *Listers) GetEndpointsLister() corev1listers.EndpointsLister {
	return corev1listers.NewEndpointsLister(l.indexerFor(&corev1.Endpoints{}))
}

func (l *Listers) GetKafkaChannelLister() messaginglisters.KafkaChannelLister {
	return messaginglisters.NewKafkaChannelLister(l.indexerFor(&v1beta1.KafkaChannel{}))
}

func (l *Listers) GetDeploymentLister() appsv1listers.DeploymentLister {
	return appsv1listers.NewDeploymentLister(l.indexerFor(&appsv1.Deployment{}))
}
