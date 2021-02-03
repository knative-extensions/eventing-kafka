package reconciler

import (
	"k8s.io/apimachinery/pkg/labels"
	v1 "k8s.io/client-go/listers/core/v1"
)

type PodIpGetter struct {
	Lister v1.PodLister
}

func (ipGetter PodIpGetter) GetAllPodsIp(namespace string, selector labels.Selector) ([]string, error) {
	pods, err := ipGetter.Lister.Pods(namespace).List(selector)
	if err != nil {
		return nil, err
	}

	ips := make([]string, 0, len(pods))

	for _, p := range pods {
		if p.Status.PodIP != "" {
			ips = append(ips, p.Status.PodIP)
		}
	}
	return ips, nil
}
