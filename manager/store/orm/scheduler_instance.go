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

type SchedulerInstanceTable struct {
	ID             uint           `gorm:"column:id;primaryKey"`
	InstanceID     string         `gorm:"column:instance_id;unique;size:63"`
	ClusterID      string         `gorm:"column:cluster_id;size:63"`
	SecurityDomain string         `gorm:"column:security_domain;size:63"`
	VIPs           string         `gorm:"column:vips;type:text;not null"`
	IDC            string         `gorm:"column:idc;size:63"`
	Location       string         `gorm:"column:location;type:text;not null"`
	NetConfig      string         `gorm:"column:net_config;type:text;not null"`
	HostName       string         `gorm:"column:host_name;size:63"`
	IP             string         `gorm:"column:ip;size:31"`
	Port           int32          `gorm:"column:port"`
	State          string         `gorm:"column:state;size:15"`
	Version        int64          `gorm:"column:version"`
	Description    string         `gorm:"column:description;size:1023"`
	CreatedAt      time.Time      `gorm:"column:created_at"`
	UpdatedAt      time.Time      `gorm:"column:updated_at"`
	DeletedAt      gorm.DeletedAt `gorm:"column:deleted_at;index"`
}

type SchedulerInstanceStore struct {
	resourceType store.ResourceType
	db           *gorm.DB
	table        string
}

func NewSchedulerInstanceStore(db *gorm.DB, table string) (store.Store, error) {
	s := &SchedulerInstanceStore{
		resourceType: store.SchedulerInstance,
		db:           db,
		table:        table,
	}

	err := s.withTable(context.Background()).AutoMigrate(&SchedulerInstanceTable{})
	if err != nil {
		return nil, err
	}

	return s, nil
}

func SchedulerInstanceToTable(t *types.SchedulerInstance) *SchedulerInstanceTable {
	return &SchedulerInstanceTable{
		InstanceID:     t.InstanceID,
		ClusterID:      t.ClusterID,
		SecurityDomain: t.SecurityDomain,
		VIPs:           t.VIPs,
		IDC:            t.IDC,
		Location:       t.Location,
		NetConfig:      t.NetConfig,
		HostName:       t.HostName,
		IP:             t.IP,
		Port:           t.Port,
		State:          t.State,
		Version:        time.Now().UnixNano(),
	}
}

func SchedulerInstanceToSchema(t *SchedulerInstanceTable) *types.SchedulerInstance {
	return &types.SchedulerInstance{
		InstanceID:     t.InstanceID,
		ClusterID:      t.ClusterID,
		SecurityDomain: t.SecurityDomain,
		VIPs:           t.VIPs,
		IDC:            t.IDC,
		Location:       t.Location,
		NetConfig:      t.NetConfig,
		HostName:       t.HostName,
		IP:             t.IP,
		Port:           t.Port,
		State:          t.State,
		CreatedAt:      t.CreatedAt.String(),
		UpdatedAt:      t.UpdatedAt.String(),
	}
}

func (s *SchedulerInstanceStore) updateSchemaToTable(new, old *SchedulerInstanceTable) *SchedulerInstanceTable {
	new.ID = old.ID
	if new.IDC != old.IDC || new.SecurityDomain != old.SecurityDomain || new.VIPs != old.VIPs || new.Port != old.Port || new.NetConfig != old.NetConfig {
		new.Version = time.Now().UnixNano()
	} else {
		new.Version = old.Version
	}
	return new
}

func (s *SchedulerInstanceStore) withTable(ctx context.Context) (tx *gorm.DB) {
	return s.db.WithContext(ctx).Table(s.table)
}

func (s *SchedulerInstanceStore) Add(ctx context.Context, id string, data interface{}, opts ...store.OpOption) (interface{}, error) {

	i, ok := data.(*types.SchedulerInstance)
	if !ok {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add scheduler instance error: reflect scheduler instance error")
	}

	instance := SchedulerInstanceToTable(i)
	tx := s.withTable(ctx).Create(instance)
	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add scheduler instance error: %s", tx.Error.Error())
	}

	return SchedulerInstanceToSchema(instance), nil
}

func (s *SchedulerInstanceStore) Delete(ctx context.Context, id string, opts ...store.OpOption) (interface{}, error) {

	instance := &SchedulerInstanceTable{}
	tx := s.withTable(ctx).Where("instance_id = ?", id).First(instance)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}

	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete scheduler instance error: %s", tx.Error.Error())
	}

	tx = tx.Delete(instance)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}

	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete scheduler instance error: %s", tx.Error.Error())
	}

	return SchedulerInstanceToSchema(instance), nil
}

func (s *SchedulerInstanceStore) Update(ctx context.Context, id string, data interface{}, opts ...store.OpOption) (interface{}, error) {
	op := store.Op{}
	op.ApplyOpts(opts)

	tInstance := &SchedulerInstanceTable{}
	tx := s.withTable(ctx).Where("instance_id = ?", id).First(tInstance)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update scheduler instance error: %s", err.Error())
	}

	i, ok := data.(*types.SchedulerInstance)
	if !ok {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update scheduler instance error: reflect scheduler instance error")
	}

	instance := SchedulerInstanceToTable(i)
	if op.Keepalive {
		tInstance.State = instance.State
		instance = tInstance
	}

	s.updateSchemaToTable(instance, tInstance)
	tx = tx.Updates(instance)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update scheduler instance error: %s", err.Error())
	} else if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update scheduler instance error: %s", tx.Error.Error())
	} else {
		return SchedulerInstanceToSchema(instance), nil
	}
}

func (s *SchedulerInstanceStore) Get(ctx context.Context, id string, opts ...store.OpOption) (interface{}, error) {
	instance := &SchedulerInstanceTable{}
	tx := s.withTable(ctx).Where("instance_id = ?", id).First(instance)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "get scheduler instance error: %s", err.Error())
	} else if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "get scheduler instance error: %s", tx.Error.Error())
	} else {
		return SchedulerInstanceToSchema(instance), nil
	}
}

func (s *SchedulerInstanceStore) List(ctx context.Context, opts ...store.OpOption) ([]interface{}, error) {
	op := store.Op{}
	op.ApplyOpts(opts)

	var instances []*SchedulerInstanceTable
	var tx *gorm.DB
	if len(op.ClusterID) <= 0 {
		tx = s.withTable(ctx).Order("instance_id").Offset(op.Marker).Limit(op.MaxItemCount).Find(&instances)
	} else {
		tx = s.withTable(ctx).Where("cluster_id = ?", op.ClusterID).Order("instance_id").Offset(op.Marker).Limit(op.MaxItemCount).Find(&instances)
	}

	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "list scheduler instances error: %s", tx.Error.Error())
	}

	var inter []interface{}
	for _, instance := range instances {
		inter = append(inter, SchedulerInstanceToSchema(instance))
	}
	return inter, nil
}
