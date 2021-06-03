package orm

import (
	"context"
	"net/url"

	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/store"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"github.com/xo/dburl"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type ormStore struct {
	db     *gorm.DB
	stores map[store.ResourceType]store.Store
	tables map[store.ResourceType]string
}

type newTableSetup func(db *gorm.DB, tableName string) (store.Store, error)

var ormTables = map[store.ResourceType]newTableSetup{
	store.SchedulerCluster:  NewSchedulerClusterStore,
	store.SchedulerInstance: NewSchedulerInstanceStore,
	store.CDNCluster:        NewCDNClusterStore,
	store.CDNInstance:       NewCDNInstanceStore,
	store.SecurityDomain:    NewSecurityDomainStore,
	store.Lease:             NewLeaseStore,
}

func newOrmStore(cfg *config.StoreConfig) (*ormStore, error) {
	if err := cfg.Valid(); err != nil {
		return nil, err
	}

	u, err := dburl.Parse("mysql://user:pass@localhost/dbname?")
	if err != nil {
		return nil, err
	}

	u.Host = cfg.Mysql.Addr
	u.Path = cfg.Mysql.Db
	u.User = url.UserPassword(cfg.Mysql.User, cfg.Mysql.Password)
	q := u.Query()
	q.Add("charset", "utf8")
	q.Add("parseTime", "True")
	q.Add("loc", "Local")
	u.RawQuery = q.Encode()
	dsn, err := dburl.GenMySQL(u)
	if err != nil {
		return nil, err
	}

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	orm := &ormStore{
		db:     db,
		stores: make(map[store.ResourceType]store.Store),
		tables: make(map[store.ResourceType]string),
	}

	for t, f := range ormTables {
		table := t.String()
		s, err := f(db, table)
		if err != nil {
			return nil, err
		}

		orm.stores[t] = s
		orm.tables[t] = table
	}

	return orm, nil
}

func NewOrmStore(cfg *config.StoreConfig) (store.Store, error) {
	return newOrmStore(cfg)
}

func (orm *ormStore) listTables() []string {
	var tables []string
	for _, t := range orm.tables {
		tables = append(tables, t)
	}

	return tables
}

func (orm *ormStore) getStore(opts ...store.OpOption) (store.Store, error) {
	op := store.Op{}
	op.ApplyOpts(opts)

	s, ok := orm.stores[op.ResourceType]
	if !ok {
		return nil, dferrors.Newf(dfcodes.InvalidResourceType, "not support resource type %s", op.ResourceType)

	}

	return s, nil
}

func (orm *ormStore) Add(ctx context.Context, id string, data interface{}, opts ...store.OpOption) (interface{}, error) {
	s, err := orm.getStore(opts...)
	if err != nil {
		return nil, err
	}

	return s.Add(ctx, id, data, opts...)
}

func (orm *ormStore) Delete(ctx context.Context, id string, opts ...store.OpOption) (interface{}, error) {
	s, err := orm.getStore(opts...)
	if err != nil {
		return nil, err
	}

	return s.Delete(ctx, id, opts...)
}

func (orm *ormStore) Update(ctx context.Context, id string, data interface{}, opts ...store.OpOption) (interface{}, error) {
	s, err := orm.getStore(opts...)
	if err != nil {
		return nil, err
	}

	return s.Update(ctx, id, data, opts...)
}

func (orm *ormStore) Get(ctx context.Context, id string, opts ...store.OpOption) (interface{}, error) {
	s, err := orm.getStore(opts...)
	if err != nil {
		return nil, err
	}

	return s.Get(ctx, id, opts...)
}

func (orm *ormStore) List(ctx context.Context, opts ...store.OpOption) ([]interface{}, error) {
	s, err := orm.getStore(opts...)
	if err != nil {
		return nil, err
	}

	return s.List(ctx, opts...)
}
