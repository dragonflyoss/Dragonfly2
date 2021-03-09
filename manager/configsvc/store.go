package configsvc

import (
	"context"
)

type Config struct {
	ID      string
	Object  string
	ObjType string
	Version uint64
	Body    []byte
}

type Store interface {
	AddConfig(ctx context.Context, id string, config *Config) (*Config, error)
	DeleteConfig(ctx context.Context, id string) (*Config, error)
	UpdateConfig(ctx context.Context, id string, config *Config) (*Config, error)
	GetConfig(ctx context.Context, id string) (*Config, error)
	ListConfigs(ctx context.Context, object string) ([]*Config, error)

	LatestConfig(ctx context.Context, object string, objType string) (*Config, error)
}

type SortConfig []*Config

func (s SortConfig) Len() int {
	return len(s)
}

func (s SortConfig) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortConfig) Less(i, j int) bool {
	return s[i].Version > s[j].Version
}
