package client

import (
	"d7y.io/dragonfly/v2/internal/dfcodes"
	"d7y.io/dragonfly/v2/internal/dferrors"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/store"
	"d7y.io/dragonfly/v2/manager/store/orm"
)

type storeSetup func(cfg *config.StoreConfig) (store.Store, error)

var storePlugins = map[string]storeSetup{
	"mysql":  orm.NewMySQLOrmStore,
	"sqlite": orm.NewSQLiteOrmStore,
}

func NewStore(cfg *config.Config) (store.Store, error) {
	if cfg.Configure.StoreName == "" {
		return nil, dferrors.Newf(dfcodes.ManagerConfigError, "config error: store-name nil")
	}

	for _, store := range cfg.Stores {
		source, err := store.Valid()
		if err != nil {
			return nil, err
		}

		if cfg.Configure.StoreName == store.Name {
			p, ok := storePlugins[source]
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
