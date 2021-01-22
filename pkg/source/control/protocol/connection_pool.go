package protocol

import (
	"context"
	"sync"
)

type ControlPlaneConnectionPool struct {
	connsLock sync.Mutex
	conns     map[string]map[string]clientServiceHolder
}

type clientServiceHolder struct {
	service  Service
	cancelFn context.CancelFunc
}

func NewControlPlaneConnectionPool() *ControlPlaneConnectionPool {
	return &ControlPlaneConnectionPool{
		conns: make(map[string]map[string]clientServiceHolder),
	}
}

func (cc *ControlPlaneConnectionPool) GetAvailableConnections(key string) []string {
	cc.connsLock.Lock()
	defer cc.connsLock.Unlock()
	var m map[string]clientServiceHolder
	var ok bool
	if m, ok = cc.conns[key]; !ok {
		return nil
	}
	hosts := make([]string, 0, len(m))
	for k, _ := range m {
		hosts = append(hosts, k)
	}
	return hosts
}

func (cc *ControlPlaneConnectionPool) ResolveControlInterface(key string, host string) (string, Service) {
	cc.connsLock.Lock()
	defer cc.connsLock.Unlock()
	if m, ok := cc.conns[key]; !ok {
		return "", nil
	} else if holder, ok := m[host]; !ok {
		return host, holder.service
	}

	return "", nil
}

func (cc *ControlPlaneConnectionPool) RemoveConnection(ctx context.Context, key string, host string) {
	cc.connsLock.Lock()
	defer cc.connsLock.Unlock()
	m, ok := cc.conns[key]
	if !ok {
		return
	}
	holder, ok := m[host]
	if !ok {
		return
	}
	holder.cancelFn()
	delete(m, host)
	if len(m) == 0 {
		delete(cc.conns, key)
	}
}

func (cc *ControlPlaneConnectionPool) DialControlService(ctx context.Context, key string, host string) (string, Service, error) {
	// Need to start new conn
	ctx, cancelFn := context.WithCancel(ctx)
	newSvc, err := StartControlClient(ctx, host)
	if err != nil {
		cancelFn()
		return "", nil, err
	}

	cc.connsLock.Lock()
	var m map[string]clientServiceHolder
	var ok bool
	if m, ok = cc.conns[key]; !ok {
		m = make(map[string]clientServiceHolder)
		cc.conns[key] = m
	}
	m[host] = clientServiceHolder{
		service:  newSvc,
		cancelFn: cancelFn,
	}
	cc.connsLock.Unlock()

	return host, newSvc, nil
}
