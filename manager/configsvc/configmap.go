package configsvc

import (
	"context"
	"sync"
)

type ConfigMap struct {
	mu      sync.Mutex
	store   Store
	configs map[string]*Config
	objects map[string]*Config
}

func NewConfigMap(store Store) *ConfigMap {
	return &ConfigMap{
		store:   store,
		configs: make(map[string]*Config),
		objects: make(map[string]*Config)}
}

func (cm *ConfigMap) AddConfig(ctx context.Context, id string, config *Config) (*Config, error) {
	if config, err := cm.store.AddConfig(ctx, id, config); err != nil {
		return nil, err
	} else {
		cm.loadToCache(ctx, id, config)
		return config, nil
	}
}

func (cm *ConfigMap) DeleteConfig(ctx context.Context, id string) (*Config, error) {
	if config, err := cm.store.DeleteConfig(ctx, id); err != nil {
		return nil, err
	} else if config != nil {
		cm.deleteCache(ctx, id, config)
		return config, nil
	} else {
		return nil, nil
	}
}

func (cm *ConfigMap) UpdateConfig(ctx context.Context, id string, config *Config) (*Config, error) {
	if config, err := cm.store.UpdateConfig(ctx, id, config); err != nil {
		return nil, err
	} else {
		cm.loadToCache(ctx, id, config)
		return config, nil
	}
}

func (cm *ConfigMap) GetConfig(ctx context.Context, id string) (*Config, error) {
	if config, exist := cm.loadFromCache(ctx, id); exist {
		return config, nil
	} else {
		if config, err := cm.store.GetConfig(ctx, id); err != nil {
			return nil, err
		} else {
			cm.loadToCache(ctx, id, config)
			return config, nil
		}
	}
}

func (cm *ConfigMap) ListConfigs(ctx context.Context, object string) ([]*Config, error) {
	if configs, err := cm.store.ListConfigs(ctx, object); err != nil {
		return nil, err
	} else {
		return configs, nil
	}
}

func (cm *ConfigMap) LatestConfig(ctx context.Context, object string, objType string) (*Config, error) {
	if config, exist := cm.latestFromCache(ctx, object, objType); exist {
		return config, nil
	} else {
		if config, err := cm.store.LatestConfig(ctx, object, objType); err != nil {
			return nil, err
		} else {
			cm.loadToCache(ctx, config.ID, config)
			return config, nil
		}
	}
}

func (cm *ConfigMap) latestFromCache(ctx context.Context, object string, objType string) (*Config, bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if config, exist := cm.objects[object]; exist {
		if config.Type == objType {
			return config, true
		}
	}

	return nil, false
}

func (cm *ConfigMap) loadFromCache(ctx context.Context, id string) (*Config, bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if config, exist := cm.configs[id]; exist {
		return config, true
	}

	return nil, false
}

func (cm *ConfigMap) loadToCache(ctx context.Context, id string, config *Config) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cur, exist := cm.configs[id]; exist {
		if cur.Version <= config.Version {
			cm.configs[id] = config
		}
	} else {
		cm.configs[id] = config
	}

	if cur, exist := cm.objects[config.Object]; exist {
		if cur.Version <= config.Version {
			cm.objects[config.Object] = config
		}
	} else {
		cm.objects[config.Object] = config
	}
}

func (cm *ConfigMap) deleteCache(ctx context.Context, id string, config *Config) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, exist := cm.configs[id]; exist {
		delete(cm.configs, id)
	}

	if _, exist := cm.objects[config.Object]; exist {
		delete(cm.objects, config.Object)
	}
}
