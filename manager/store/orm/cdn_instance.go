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

type CDNInstanceTable struct {
	ID         uint           `gorm:"column:id;primaryKey"`
	InstanceID string         `gorm:"column:instance_id;unique;size:63"`
	ClusterID  string         `gorm:"column:cluster_id;size:63"`
	IDC        string         `gorm:"column:idc;size:63"`
	Location   string         `gorm:"column:location;size:4095"`
	HostName   string         `gorm:"column:host_name;size:63"`
	IP         string         `gorm:"column:ip;size:31"`
	Port       int32          `gorm:"column:port"`
	RPCPort    int32          `gorm:"column:rpc_port"`
	DownPort   int32          `gorm:"column:down_port"`
	State      string         `gorm:"column:state;size:15"`
	Version    int64          `gorm:"column:version"`
	CreatedAt  time.Time      `gorm:"column:created_at"`
	UpdatedAt  time.Time      `gorm:"column:updated_at"`
	DeletedAt  gorm.DeletedAt `gorm:"column:deleted_at;index"`
}

type CDNInstanceStore struct {
	resourceType store.ResourceType
	db           *gorm.DB
	table        string
}

func NewCDNInstanceStore(db *gorm.DB, table string) (store.Store, error) {
	s := &CDNInstanceStore{
		resourceType: store.CDNInstance,
		db:           db,
		table:        table,
	}

	err := s.withTable(context.Background()).AutoMigrate(&CDNInstanceTable{})
	if err != nil {
		return nil, err
	}

	return s, nil
}

func CDNInstanceToTable(t *types.CDNInstance) *CDNInstanceTable {
	return &CDNInstanceTable{
		InstanceID: t.InstanceID,
		ClusterID:  t.ClusterID,
		IDC:        t.IDC,
		Location:   t.Location,
		HostName:   t.HostName,
		IP:         t.IP,
		Port:       t.Port,
		RPCPort:    t.RPCPort,
		DownPort:   t.DownPort,
		State:      t.State,
		Version:    time.Now().UnixNano(),
	}
}

func CDNInstanceToSchema(t *CDNInstanceTable) *types.CDNInstance {
	return &types.CDNInstance{
		InstanceID: t.InstanceID,
		ClusterID:  t.ClusterID,
		IDC:        t.IDC,
		Location:   t.Location,
		HostName:   t.HostName,
		IP:         t.IP,
		Port:       t.Port,
		RPCPort:    t.RPCPort,
		DownPort:   t.DownPort,
		State:      t.State,
		CreatedAt:  t.CreatedAt.String(),
		UpdatedAt:  t.UpdatedAt.String(),
	}
}

func (s *CDNInstanceStore) updateSchemaToTable(new, old *CDNInstanceTable) *CDNInstanceTable {
	new.ID = old.ID
	if new.IDC != old.IDC || new.Port != old.Port || new.DownPort != old.DownPort || new.RPCPort != old.RPCPort {
		new.Version = time.Now().UnixNano()
	} else {
		new.Version = old.Version
	}
	return new
}

func (s *CDNInstanceStore) withTable(ctx context.Context) (tx *gorm.DB) {
	return s.db.WithContext(ctx).Table(s.table)
}

func (s *CDNInstanceStore) Add(ctx context.Context, id string, data interface{}, opts ...store.OpOption) (interface{}, error) {
	i, ok := data.(*types.CDNInstance)
	if !ok {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add cdn instance error: reflect cdn instance error")
	}

	instance := CDNInstanceToTable(i)
	tx := s.withTable(ctx).Create(instance)
	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add cdn instance error: %s", tx.Error.Error())
	}

	return CDNInstanceToSchema(instance), nil
}

func (s *CDNInstanceStore) Delete(ctx context.Context, id string, opts ...store.OpOption) (interface{}, error) {
	instance := &CDNInstanceTable{}
	tx := s.withTable(ctx).Where("instance_id = ?", id).First(instance)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}

	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete cdn instance error: %s", tx.Error.Error())
	}

	tx = tx.Delete(instance)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}

	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete cdn instance error: %s", tx.Error.Error())
	}

	return CDNInstanceToSchema(instance), nil
}

func (s *CDNInstanceStore) Update(ctx context.Context, id string, data interface{}, opts ...store.OpOption) (interface{}, error) {
	tInstance := &CDNInstanceTable{}
	tx := s.withTable(ctx).Where("instance_id = ?", id).First(tInstance)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update cdn instance error: %s", err.Error())
	}

	i, ok := data.(*types.CDNInstance)
	if !ok {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update cdn instance error: reflect cdn instance error")
	}

	instance := CDNInstanceToTable(i)
	s.updateSchemaToTable(instance, tInstance)
	tx = tx.Updates(instance)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update cdn instance error: %s", err.Error())
	} else if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update cdn instance error: %s", tx.Error.Error())
	} else {
		return CDNInstanceToSchema(instance), nil
	}
}

func (s *CDNInstanceStore) Get(ctx context.Context, id string, opts ...store.OpOption) (interface{}, error) {
	instance := &CDNInstanceTable{}
	tx := s.withTable(ctx).Where("instance_id = ?", id).First(instance)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "get cdn instance error: %s", err.Error())
	} else if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "get cdn instance error: %s", tx.Error.Error())
	} else {
		return CDNInstanceToSchema(instance), nil
	}
}

func (s *CDNInstanceStore) List(ctx context.Context, opts ...store.OpOption) ([]interface{}, error) {
	op := store.Op{}
	op.ApplyOpts(opts)

	var instances []*CDNInstanceTable
	var tx *gorm.DB
	if len(op.ClusterID) <= 0 {
		tx = s.withTable(ctx).Order("instance_id").Offset(op.Marker).Limit(op.MaxItemCount).Find(&instances)
	} else {
		tx = s.withTable(ctx).Where("cluster_id = ?", op.ClusterID).Order("instance_id").Offset(op.Marker).Limit(op.MaxItemCount).Find(&instances)
	}

	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "list cdn instances error %s", tx.Error.Error())
	}

	var inter []interface{}
	for _, instance := range instances {
		inter = append(inter, CDNInstanceToSchema(instance))
	}

	return inter, nil
}
