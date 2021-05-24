package orm

import (
	"context"
	"errors"
	"time"

	"d7y.io/dragonfly/v2/manager/apis/v2/types"
	"d7y.io/dragonfly/v2/manager/store"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"gorm.io/gorm"
)

type CDNClusterTable struct {
	ID        uint           `gorm:"column:id;primaryKey"`
	ClusterID string         `gorm:"column:cluster_id;unique;size:63"`
	Config    string         `gorm:"column:config;size:4095"`
	Creator   string         `gorm:"column:creator;size:31"`
	Modifier  string         `gorm:"column:modifier;size:31"`
	Version   int64          `gorm:"column:version"`
	CreatedAt time.Time      `gorm:"column:created_at"`
	UpdatedAt time.Time      `gorm:"column:updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"column:deleted_at;index"`
}

type CDNClusterStore struct {
	resourceType store.ResourceType
	db           *gorm.DB
	table        string
}

func NewCDNClusterStore(db *gorm.DB, table string) (store.Store, error) {
	s := &CDNClusterStore{
		resourceType: store.CDNCluster,
		db:           db,
		table:        table,
	}

	err := s.withTable(context.Background()).AutoMigrate(&CDNClusterTable{})
	if err != nil {
		return nil, err
	}

	return s, nil
}

func CDNClusterToTable(t *types.CDNCluster) *CDNClusterTable {
	return &CDNClusterTable{
		ClusterID: t.ClusterID,
		Config:    t.Config,
		Creator:   t.Creator,
		Modifier:  t.Modifier,
		Version:   time.Now().UnixNano(),
	}
}

func CDNClusterToSchema(t *CDNClusterTable) *types.CDNCluster {
	return &types.CDNCluster{
		ClusterID: t.ClusterID,
		Config:    t.Config,
		Creator:   t.Creator,
		Modifier:  t.Modifier,
		CreatedAt: t.CreatedAt.String(),
		UpdatedAt: t.UpdatedAt.String(),
	}
}

func (s *CDNClusterStore) updateSchemaToTable(new, old *CDNClusterTable) *CDNClusterTable {
	new.ID = old.ID
	if new.Config != old.Config {
		new.Version = time.Now().UnixNano()
	} else {
		new.Version = old.Version
	}
	return new
}

func (s *CDNClusterStore) withTable(ctx context.Context) (tx *gorm.DB) {
	return s.db.WithContext(ctx).Table(s.table)
}

func (s *CDNClusterStore) Add(ctx context.Context, id string, data interface{}, opts ...store.OpOption) (interface{}, error) {
	c, ok := data.(*types.CDNCluster)
	if !ok {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add cdn cluster error: reflect cdn cluster error")
	}

	cluster := CDNClusterToTable(c)
	tx := s.withTable(ctx).Create(cluster)
	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add cdn cluster error: %s", tx.Error.Error())
	}

	return CDNClusterToSchema(cluster), nil
}

func (s *CDNClusterStore) Delete(ctx context.Context, id string, opts ...store.OpOption) (interface{}, error) {
	cluster := &CDNClusterTable{}
	tx := s.withTable(ctx).Where("cluster_id = ?", id).First(cluster)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}

	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete cdn cluster error: %s", tx.Error.Error())
	}

	tx = tx.Delete(cluster)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}

	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete cdn cluster error: %s", tx.Error.Error())
	}

	return CDNClusterToSchema(cluster), nil
}

func (s *CDNClusterStore) Update(ctx context.Context, id string, data interface{}, opts ...store.OpOption) (interface{}, error) {
	tCluster := &CDNClusterTable{}
	tx := s.withTable(ctx).Where("cluster_id = ?", id).First(tCluster)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update cdn cluster error: %s", err.Error())
	}

	c, ok := data.(*types.CDNCluster)
	if !ok {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update cdn cluster error: reflect cdn cluster error")
	}

	cluster := CDNClusterToTable(c)
	s.updateSchemaToTable(cluster, tCluster)
	tx = tx.Updates(cluster)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update cdn cluster error: %s", err.Error())
	} else if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update cdn cluster error: %s", tx.Error.Error())
	} else {
		return CDNClusterToSchema(cluster), nil
	}
}

func (s *CDNClusterStore) Get(ctx context.Context, id string, opts ...store.OpOption) (interface{}, error) {
	cluster := &CDNClusterTable{}
	tx := s.withTable(ctx).Where("cluster_id = ?", id).First(cluster)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "get cdn cluster error: %s", err.Error())
	} else if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "get cdn cluster error: %s", tx.Error.Error())
	} else {
		return CDNClusterToSchema(cluster), nil
	}
}

func (s *CDNClusterStore) List(ctx context.Context, opts ...store.OpOption) ([]interface{}, error) {
	op := store.Op{}
	op.ApplyOpts(opts)

	var clusters []*CDNClusterTable
	tx := s.withTable(ctx).Order("cluster_id").Offset(op.Marker).Limit(op.MaxItemCount).Find(&clusters)
	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "list cnd clusters error %s", tx.Error.Error())
	}

	var inter []interface{}
	for _, cluster := range clusters {
		inter = append(inter, CDNClusterToSchema(cluster))
	}

	return inter, nil
}
