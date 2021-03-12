package configsvc

import (
	"context"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"sort"
	"sync"
)

type memoryStore struct {
	mu      sync.Mutex
	configs map[string]*Config
	objects map[string]*Config
}

func NewMemoryStore() Store {
	return &memoryStore{
		configs: make(map[string]*Config),
		objects: make(map[string]*Config),
	}
}

func (memory *memoryStore) AddConfig(ctx context.Context, id string, config *Config) (*Config, error) {
	memory.mu.Lock()
	defer memory.mu.Unlock()

	if _, exist := memory.configs[id]; exist {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add config error: id %s", id)
	} else {
		config.ID = id
		memory.configs[id] = config

		if objConfig, exist := memory.objects[config.Object]; exist {
			if config.Version > objConfig.Version {
				delete(memory.objects, config.Object)
				memory.objects[config.Object] = config
			}
		} else {
			memory.objects[config.Object] = config
		}

		return config, nil
	}
}

func (memory *memoryStore) DeleteConfig(ctx context.Context, id string) (*Config, error) {
	memory.mu.Lock()
	defer memory.mu.Unlock()

	if config, exist := memory.configs[id]; !exist {
		return nil, nil
	} else {
		delete(memory.configs, id)

		if objConfig, exist := memory.objects[config.Object]; exist {
			if objConfig.Version == config.Version {
				delete(memory.objects, config.Object)
			}
		}

		return config, nil
	}
}

func (memory *memoryStore) UpdateConfig(ctx context.Context, id string, config *Config) (*Config, error) {
	memory.mu.Lock()
	defer memory.mu.Unlock()

	if _, exist := memory.configs[id]; !exist {
		return nil, dferrors.Newf(dfcodes.ManagerConfigNotFound, "update config error: id %s", id)
	} else {
		config.ID = id
		memory.configs[id] = config

		if objConfig, exist := memory.objects[config.Object]; exist {
			if objConfig.Version <= config.Version {
				memory.objects[config.Object] = config
			}
		}

		return config, nil
	}
}

func (memory *memoryStore) GetConfig(ctx context.Context, id string) (*Config, error) {
	memory.mu.Lock()
	defer memory.mu.Unlock()

	if config, exist := memory.configs[id]; exist {
		return config, nil
	} else {
		return nil, dferrors.Newf(dfcodes.ManagerConfigNotFound, "get config error: id %s", id)
	}
}

func (memory *memoryStore) ListConfigs(ctx context.Context, object string) ([]*Config, error) {
	memory.mu.Lock()
	defer memory.mu.Unlock()

	return memory.listSortedConfig(ctx, object, 0)
}

func (memory *memoryStore) LatestConfig(ctx context.Context, object string, objType string) (*Config, error) {
	memory.mu.Lock()
	defer memory.mu.Unlock()

	if config, exist := memory.objects[object]; !exist {
		configs, err := memory.listSortedConfig(ctx, object, 1)
		if err != nil {
			return nil, err
		}

		memory.objects[object] = configs[0]
		return configs[0], nil
	} else {
		if config.Type == objType {
			return config, nil
		} else {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "latest config error: object %s, objType %s", object, objType)
		}
	}
}

func (memory *memoryStore) listSortedConfig(ctx context.Context, object string, maxLen uint32) ([]*Config, error) {
	configs := make([]*Config, 0)
	for _, config := range memory.configs {
		if config.Object == object {
			configs = append(configs, config)
		}
	}

	if len(configs) <= 0 {
		return nil, dferrors.Newf(dfcodes.ManagerConfigNotFound, "list sorted config error: object %s, maxLen %d", object, maxLen)
	}

	sort.Sort(SortConfig(configs))
	len := uint32(len(configs))
	if maxLen == 0 || len <= maxLen {
		return configs, nil
	} else {
		return configs[0:maxLen], nil
	}
}
