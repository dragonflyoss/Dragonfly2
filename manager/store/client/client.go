package client

import (
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/store"
	"d7y.io/dragonfly/v2/manager/store/orm"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
)

type storeSetup func(cfg *config.StoreConfig) (store.Store, error)

var storePlugins = map[string]storeSetup{
	"mysql": orm.NewOrmStore,
}

func NewStore(cfg *config.Config) (store.Store, error) {
	if cfg.Configure.StoreName == "" {
		return nil, dferrors.Newf(dfcodes.ManagerConfigError, "config error: store-name nil")
	}

	for _, store := range cfg.Stores {
		if cfg.Configure.StoreName == store.Name {
			p, ok := storePlugins[store.Type]
			if ok {
				s, err := p(store)
				if err != nil {
					return nil, dferrors.Newf(dfcodes.ManagerConfigError, "setup error: %s", err.Error())
				}

				return s, nil
			}
		}
	}

	return nil, dferrors.Newf(dfcodes.ManagerConfigError, "config error: not find matched store")
}
