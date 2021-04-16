package fake

import (
	"fmt"
)

type configMapData map[string]string

type FakeConfigmapLoader struct {
	registered map[string]configMapData
}

func NewFakeConfigmapLoader() *FakeConfigmapLoader {
	return &FakeConfigmapLoader{
		registered: map[string]configMapData{},
	}
}

func (f *FakeConfigmapLoader) Load(path string) (map[string]string, error) {
	if val, ok := f.registered[path]; ok {
		return val, nil
	}
	return nil, fmt.Errorf("given path %s is not registered", path)
}

func (f *FakeConfigmapLoader) Register(path string, data map[string]string) {
	f.registered[path] = data
}
