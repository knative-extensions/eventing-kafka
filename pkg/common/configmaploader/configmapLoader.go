package configmaploader

import (
	"context"
	"errors"
)

type ConfigmapLoader func(path string) (map[string]string, error)

// Key is used as the key for associating information with a context.Context.
type Key struct{}

// FromContext obtains the ConfigmapLoader contained in a Context, if present
func FromContext(ctx context.Context) (ConfigmapLoader, error) {
	untyped := ctx.Value(Key{})
	if untyped == nil {
		return nil, errors.New("ConfigmapLoader does not exist in context")
	}
	return untyped.(func(data string) (map[string]string, error)), nil
}
