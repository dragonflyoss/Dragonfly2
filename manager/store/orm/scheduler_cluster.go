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

type SchedulerClusterTable struct {
	ID           uint           `gorm:"column:id;primaryKey"`
	ClusterID    string         `gorm:"column:cluster_id;unique;size:63"`
	Config       string         `gorm:"column:config;type:text;not null"`
	ClientConfig string         `gorm:"column:client_config;type:text;not null"`
	Creator      string         `gorm:"column:creator;size:31"`
	Modifier     string         `gorm:"column:modifier;size:31"`
	Enable       bool           `gorm:"column:enable;type:bool"`
	Version      int64          `gorm:"column:version"`
	Description  string         `gorm:"column:description;size:1023"`
	CreatedAt    time.Time      `gorm:"column:created_at"`
	UpdatedAt    time.Time      `gorm:"column:updated_at"`
	DeletedAt    gorm.DeletedAt `gorm:"column:deleted_at;index"`
}

type SchedulerClusterStore struct {
	resourceType store.ResourceType
	db           *gorm.DB
	table        string
}

func NewSchedulerClusterStore(db *gorm.DB, table string) (store.Store, error) {
	s := &SchedulerClusterStore{
		resourceType: store.SchedulerCluster,
		db:           db,
		table:        table,
	}

	err := s.withTable(context.Background()).AutoMigrate(&SchedulerClusterTable{})
	if err != nil {
		return nil, err
	}

	return s, nil
}

func SchedulerClusterToTable(t *types.SchedulerCluster) *SchedulerClusterTable {
	return &SchedulerClusterTable{
		ClusterID:    t.ClusterID,
		Config:       t.Config,
		ClientConfig: t.ClientConfig,
		Creator:      t.Creator,
		Modifier:     t.Modifier,
		Version:      time.Now().UnixNano(),
	}
}

func SchedulerClusterToSchema(t *SchedulerClusterTable) *types.SchedulerCluster {
	return &types.SchedulerCluster{
		ClusterID:    t.ClusterID,
		Config:       t.Config,
		ClientConfig: t.ClientConfig,
		Creator:      t.Creator,
		Modifier:     t.Modifier,
		CreatedAt:    t.CreatedAt.String(),
		UpdatedAt:    t.UpdatedAt.String(),
	}
}

func (s *SchedulerClusterStore) updateSchemaToTable(new, old *SchedulerClusterTable) *SchedulerClusterTable {
	new.ID = old.ID
	if new.Config != old.Config || new.ClientConfig != old.ClientConfig {
		new.Version = time.Now().UnixNano()
	} else {
		new.Version = old.Version
	}
	return new
}

func (s *SchedulerClusterStore) withTable(ctx context.Context) (tx *gorm.DB) {
	return s.db.WithContext(ctx).Table(s.table)
}

func (s *SchedulerClusterStore) Add(ctx context.Context, id string, data interface{}, opts ...store.OpOption) (interface{}, error) {
	c, ok := data.(*types.SchedulerCluster)
	if !ok {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add scheduler cluster error: reflect scheduler cluster error")
	}

	cluster := SchedulerClusterToTable(c)
	tx := s.withTable(ctx).Create(cluster)
	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add scheduler cluster error: %s", tx.Error.Error())
	}

	return SchedulerClusterToSchema(cluster), nil
}

func (s *SchedulerClusterStore) Delete(ctx context.Context, id string, opts ...store.OpOption) (interface{}, error) {
	cluster := &SchedulerClusterTable{}
	tx := s.withTable(ctx).Where("cluster_id = ?", id).First(cluster)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}

	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete scheduler cluster error: %s", tx.Error.Error())
	}

	tx = tx.Delete(cluster)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}

	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete scheduler cluster error: %s", tx.Error.Error())
	}

	return SchedulerClusterToSchema(cluster), nil
}

func (s *SchedulerClusterStore) Update(ctx context.Context, id string, data interface{}, opts ...store.OpOption) (interface{}, error) {
	tCluster := &SchedulerClusterTable{}
	tx := s.withTable(ctx).Where("cluster_id = ?", id).First(tCluster)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update scheduler cluster error: %s", err.Error())
	}

	c, ok := data.(*types.SchedulerCluster)
	if !ok {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update scheduler cluster error: reflect scheduler cluster error")
	}

	cluster := SchedulerClusterToTable(c)
	s.updateSchemaToTable(cluster, tCluster)
	tx = tx.Updates(cluster)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update scheduler cluster error: %s", err.Error())
	} else if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update scheduler cluster error: %s", tx.Error.Error())
	} else {
		return SchedulerClusterToSchema(cluster), nil
	}
}

func (s *SchedulerClusterStore) Get(ctx context.Context, id string, opts ...store.OpOption) (interface{}, error) {
	cluster := &SchedulerClusterTable{}
	tx := s.withTable(ctx).Where("cluster_id = ?", id).First(cluster)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "get scheduler cluster error: %s", err.Error())
	} else if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "get scheduler cluster error: %s", tx.Error.Error())
	} else {
		return SchedulerClusterToSchema(cluster), nil
	}
}

func (s *SchedulerClusterStore) List(ctx context.Context, opts ...store.OpOption) ([]interface{}, error) {
	op := store.Op{}
	op.ApplyOpts(opts)

	var clusters []*SchedulerClusterTable
	tx := s.withTable(ctx).Order("cluster_id").Offset(op.Marker).Limit(op.MaxItemCount).Find(&clusters)
	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "list scheduler clusters error: %s", tx.Error.Error())
	}

	var inter []interface{}
	for _, cluster := range clusters {
		inter = append(inter, SchedulerClusterToSchema(cluster))
	}

	return inter, nil
}
