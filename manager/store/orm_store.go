package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"d7y.io/dragonfly/v2/manager/apis/v2/types"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type SchedulerClusterTable struct {
	ID              uint           `gorm:"primaryKey"`
	ClusterId       string         `gorm:"unique;size:63"`
	SchedulerConfig string         `gorm:"size:4095"`
	ClientConfig    string         `gorm:"size:4095"`
	Creator         string         `gorm:"size:31"`
	Modifier        string         `gorm:"size:31"`
	Version         int64          `gorm:"version"`
	CreatedAt       time.Time      `gorm:"created_at"`
	UpdatedAt       time.Time      `gorm:"updated_at"`
	DeletedAt       gorm.DeletedAt `gorm:"deleted_at;index"`
}

type SchedulerInstanceTable struct {
	ID             uint           `gorm:"primaryKey"`
	InstanceId     string         `gorm:"unique;size:63"`
	ClusterId      string         `gorm:"size:63"`
	SecurityDomain string         `gorm:"size:63"`
	Vips           string         `gorm:"size:4095"`
	Idc            string         `gorm:"size:63"`
	Location       string         `gorm:"size:4095"`
	NetConfig      string         `gorm:"size:4095"`
	HostName       string         `gorm:"size:63"`
	Ip             string         `gorm:"size:31"`
	Port           int32          `gorm:"port"`
	State          string         `gorm:"size:15"`
	Version        int64          `gorm:"version"`
	CreatedAt      time.Time      `gorm:"created_at"`
	UpdatedAt      time.Time      `gorm:"updated_at"`
	DeletedAt      gorm.DeletedAt `gorm:"deleted_at;index"`
}

type CdnClusterTable struct {
	ID        uint           `gorm:"primaryKey"`
	ClusterId string         `gorm:"unique;size:63"`
	Config    string         `gorm:"size:4095"`
	Creator   string         `gorm:"size:31"`
	Modifier  string         `gorm:"size:31"`
	Version   int64          `gorm:"version"`
	CreatedAt time.Time      `gorm:"created_at"`
	UpdatedAt time.Time      `gorm:"updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"deleted_at;index"`
}

type CdnInstanceTable struct {
	ID         uint           `gorm:"primaryKey"`
	InstanceId string         `gorm:"unique;size:63"`
	ClusterId  string         `gorm:"size:63"`
	Idc        string         `gorm:"size:63"`
	Location   string         `gorm:"size:4095"`
	HostName   string         `gorm:"size:63"`
	Ip         string         `gorm:"size:31"`
	Port       int32          `gorm:"port"`
	RpcPort    int32          `gorm:"rpc_port"`
	DownPort   int32          `gorm:"down_port"`
	State      string         `gorm:"size:15"`
	Version    int64          `gorm:"version"`
	CreatedAt  time.Time      `gorm:"created_at"`
	UpdatedAt  time.Time      `gorm:"updated_at"`
	DeletedAt  gorm.DeletedAt `gorm:"deleted_at;index"`
}

type SecurityDomainTable struct {
	ID             uint           `gorm:"primaryKey"`
	SecurityDomain string         `gorm:"unique;size:63"`
	DisplayName    string         `gorm:"size:63"`
	ProxyDomain    string         `gorm:"size:4095"`
	Creator        string         `gorm:"size:31"`
	Modifier       string         `gorm:"size:31"`
	Version        int64          `gorm:"version"`
	CreatedAt      time.Time      `gorm:"created_at"`
	UpdatedAt      time.Time      `gorm:"updated_at"`
	DeletedAt      gorm.DeletedAt `gorm:"deleted_at;index"`
}

type WarmupTaskTable struct {
	ID          uint           `gorm:"primaryKey"`
	TaskId      string         `gorm:"unique;size:63"`
	ClusterId   string         `gorm:"size:63"`
	Type        string         `gorm:"size:31"`
	OriginalUri string         `gorm:"size:1023"`
	State       string         `gorm:"size:15"`
	TaskUris    string         `gorm:"size:4095"`
	CreatedAt   time.Time      `gorm:"created_at"`
	UpdatedAt   time.Time      `gorm:"created_at"`
	DeletedAt   gorm.DeletedAt `gorm:"index"`
}

type ormStore struct {
	db *gorm.DB
}

func NewOrmStore(cfg *config.StoreConfig) (Store, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local",
		cfg.Mysql.User, cfg.Mysql.Password, cfg.Mysql.IP, cfg.Mysql.Port, cfg.Mysql.Db)

	if db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{}); err != nil {
		return nil, err
	} else {
		orm := &ormStore{
			db: db,
		}

		if err := orm.withTable(context.TODO(), SchedulerCluster).AutoMigrate(&SchedulerClusterTable{}); err != nil {
			return nil, err
		}

		if err := orm.withTable(context.TODO(), SchedulerInstance).AutoMigrate(&SchedulerInstanceTable{}); err != nil {
			return nil, err
		}

		if err := orm.withTable(context.TODO(), CdnCluster).AutoMigrate(&CdnClusterTable{}); err != nil {
			return nil, err
		}

		if err := orm.withTable(context.TODO(), CdnInstance).AutoMigrate(&CdnInstanceTable{}); err != nil {
			return nil, err
		}

		if err := orm.withTable(context.TODO(), SecurityDomain).AutoMigrate(&SecurityDomainTable{}); err != nil {
			return nil, err
		}

		return orm, nil
	}
}

func (orm *ormStore) withTable(ctx context.Context, resourceType ResourceType) (tx *gorm.DB) {
	switch resourceType {
	case SchedulerCluster, SchedulerInstance, CdnCluster, CdnInstance, SecurityDomain:
		return orm.db.WithContext(ctx).Table(resourceType.String())
	default:
		return orm.db.WithContext(ctx)
	}
}

func schemaToTable(data interface{}) interface{} {
	switch t := data.(type) {
	case *types.SchedulerCluster:
		return &SchedulerClusterTable{
			ClusterId:       t.ClusterId,
			SchedulerConfig: t.SchedulerConfig,
			ClientConfig:    t.ClientConfig,
			Creator:         t.Creator,
			Modifier:        t.Modifier,
			Version:         time.Now().UnixNano(),
		}
	case *types.SchedulerInstance:
		return &SchedulerInstanceTable{
			InstanceId:     t.InstanceId,
			ClusterId:      t.ClusterId,
			SecurityDomain: t.SecurityDomain,
			Vips:           t.Vips,
			Idc:            t.Idc,
			Location:       t.Location,
			NetConfig:      t.NetConfig,
			HostName:       t.HostName,
			Ip:             t.Ip,
			Port:           t.Port,
			State:          t.State,
			Version:        time.Now().UnixNano(),
		}
	case *types.CdnCluster:
		return &CdnClusterTable{
			ClusterId: t.ClusterId,
			Config:    t.Config,
			Creator:   t.Creator,
			Modifier:  t.Modifier,
			Version:   time.Now().UnixNano(),
		}
	case *types.CdnInstance:
		return &CdnInstanceTable{
			InstanceId: t.InstanceId,
			ClusterId:  t.ClusterId,
			Idc:        t.Idc,
			Location:   t.Location,
			HostName:   t.HostName,
			Ip:         t.Ip,
			Port:       t.Port,
			RpcPort:    t.RpcPort,
			DownPort:   t.DownPort,
			State:      t.State,
			Version:    time.Now().UnixNano(),
		}
	case *types.SecurityDomain:
		return &SecurityDomainTable{
			SecurityDomain: t.SecurityDomain,
			DisplayName:    t.DisplayName,
			ProxyDomain:    t.ProxyDomain,
			Creator:        t.Creator,
			Modifier:       t.Modifier,
			Version:        time.Now().UnixNano(),
		}
	default:
		return nil
	}
}

func tableToSchema(data interface{}) interface{} {
	switch t := data.(type) {
	case *SchedulerClusterTable:
		return &types.SchedulerCluster{
			ClusterId:       t.ClusterId,
			SchedulerConfig: t.SchedulerConfig,
			ClientConfig:    t.ClientConfig,
			Creator:         t.Creator,
			Modifier:        t.Modifier,
			CreatedAt:       t.CreatedAt.String(),
			UpdatedAt:       t.UpdatedAt.String(),
		}
	case *SchedulerInstanceTable:
		return &types.SchedulerInstance{
			InstanceId:     t.InstanceId,
			ClusterId:      t.ClusterId,
			SecurityDomain: t.SecurityDomain,
			Vips:           t.Vips,
			Idc:            t.Idc,
			Location:       t.Location,
			NetConfig:      t.NetConfig,
			HostName:       t.HostName,
			Ip:             t.Ip,
			Port:           t.Port,
			State:          t.State,
			CreatedAt:      t.CreatedAt.String(),
			UpdatedAt:      t.UpdatedAt.String(),
		}
	case *CdnClusterTable:
		return &types.CdnCluster{
			ClusterId: t.ClusterId,
			Config:    t.Config,
			Creator:   t.Creator,
			Modifier:  t.Modifier,
			CreatedAt: t.CreatedAt.String(),
			UpdatedAt: t.UpdatedAt.String(),
		}
	case *CdnInstanceTable:
		return &types.CdnInstance{
			InstanceId: t.InstanceId,
			ClusterId:  t.ClusterId,
			Idc:        t.Idc,
			Location:   t.Location,
			HostName:   t.HostName,
			Ip:         t.Ip,
			Port:       t.Port,
			RpcPort:    t.RpcPort,
			DownPort:   t.DownPort,
			State:      t.State,
			CreatedAt:  t.CreatedAt.String(),
			UpdatedAt:  t.UpdatedAt.String(),
		}
	case *SecurityDomainTable:
		return &types.SecurityDomain{
			SecurityDomain: t.SecurityDomain,
			DisplayName:    t.DisplayName,
			ProxyDomain:    t.ProxyDomain,
			Creator:        t.Creator,
			Modifier:       t.Modifier,
			CreatedAt:      t.CreatedAt.String(),
			UpdatedAt:      t.UpdatedAt.String(),
		}
	default:
		return nil
	}
}

func updateSchemaToTable(new, old interface{}) interface{} {
	switch newD := new.(type) {
	case *SchedulerClusterTable:
		oldD := old.(*SchedulerClusterTable)
		newD.ID = oldD.ID
		if newD.SchedulerConfig != oldD.SchedulerConfig || newD.ClientConfig != oldD.ClientConfig {
			newD.Version = time.Now().UnixNano()
		} else {
			newD.Version = oldD.Version
		}
		return newD
	case *SchedulerInstanceTable:
		oldD := old.(*SchedulerInstanceTable)
		newD.ID = oldD.ID
		if newD.Idc != oldD.Idc || newD.SecurityDomain != oldD.SecurityDomain || newD.Vips != oldD.Vips || newD.Port != oldD.Port || newD.NetConfig != oldD.NetConfig {
			newD.Version = time.Now().UnixNano()
		} else {
			newD.Version = oldD.Version
		}
		return newD
	case *CdnClusterTable:
		oldD := old.(*CdnClusterTable)
		newD.ID = oldD.ID
		if newD.Config != oldD.Config {
			newD.Version = time.Now().UnixNano()
		} else {
			newD.Version = oldD.Version
		}
		return newD
	case *CdnInstanceTable:
		oldD := old.(*CdnInstanceTable)
		newD.ID = oldD.ID
		if newD.Idc != oldD.Idc || newD.Port != oldD.Port || newD.DownPort != oldD.DownPort || newD.RpcPort != oldD.RpcPort {
			newD.Version = time.Now().UnixNano()
		} else {
			newD.Version = oldD.Version
		}
		return newD
	case *SecurityDomainTable:
		oldD := old.(*SecurityDomainTable)
		newD.ID = oldD.ID
		if newD.ProxyDomain != oldD.ProxyDomain || newD.DisplayName != oldD.DisplayName {
			newD.Version = time.Now().UnixNano()
		} else {
			newD.Version = oldD.Version
		}
		return newD
	default:
		return nil
	}
}

func (orm *ormStore) Add(ctx context.Context, id string, data interface{}, opts ...OpOption) (interface{}, error) {
	op := Op{}
	op.ApplyOpts(opts)

	switch op.ResourceType {
	case SchedulerCluster:
		c, ok := data.(*types.SchedulerCluster)
		if !ok {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add scheduler cluster error: reflect scheduler cluster error")
		}

		cluster := schemaToTable(c)
		tx := orm.withTable(ctx, op.ResourceType).Create(cluster)
		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add scheduler cluster error: %s", tx.Error.Error())
		} else {
			return tableToSchema(cluster), nil
		}
	case SchedulerInstance:
		i, ok := data.(*types.SchedulerInstance)
		if !ok {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add scheduler instance error: reflect scheduler instance error")
		}

		instance := schemaToTable(i)
		tx := orm.withTable(ctx, op.ResourceType).Create(instance)
		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add scheduler instance error: %s", tx.Error.Error())
		} else {
			return tableToSchema(instance), nil
		}
	case CdnCluster:
		c, ok := data.(*types.CdnCluster)
		if !ok {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add cdn cluster error: reflect cdn cluster error")
		}

		cluster := schemaToTable(c)
		tx := orm.withTable(ctx, op.ResourceType).Create(cluster)
		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add cdn instance error: %s", tx.Error.Error())
		} else {
			return tableToSchema(cluster), nil
		}
	case CdnInstance:
		i, ok := data.(*types.CdnInstance)
		if !ok {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add cdn instance error: reflect cdn instance error")
		}

		instance := schemaToTable(i)
		tx := orm.withTable(ctx, op.ResourceType).Create(instance)
		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add cdn instance error: %s", tx.Error.Error())
		} else {
			return tableToSchema(instance), nil
		}
	case SecurityDomain:
		d, ok := data.(*types.SecurityDomain)
		if !ok {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add security domain error: reflect security domain error")
		}

		domain := schemaToTable(d)
		tx := orm.withTable(ctx, op.ResourceType).Create(domain)
		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add security domain error: %s", tx.Error.Error())
		} else {
			return tableToSchema(domain), nil
		}
	default:
		return nil, dferrors.Newf(dfcodes.InvalidResourceType, "add store error: not support source type %s", op.ResourceType)
	}
}

func (orm *ormStore) Delete(ctx context.Context, id string, opts ...OpOption) (interface{}, error) {
	op := Op{}
	op.ApplyOpts(opts)

	switch op.ResourceType {
	case SchedulerCluster:
		cluster := &SchedulerClusterTable{}
		tx := orm.withTable(ctx, op.ResourceType).Where("cluster_id = ?", id).First(cluster)
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

		return tableToSchema(cluster), nil
	case SchedulerInstance:
		instance := &SchedulerInstanceTable{}
		tx := orm.withTable(ctx, op.ResourceType).Where("instance_id = ?", id).First(instance)
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

		return tableToSchema(instance), nil
	case CdnCluster:
		cluster := &CdnClusterTable{}
		tx := orm.withTable(ctx, op.ResourceType).Where("cluster_id = ?", id).First(cluster)
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

		return tableToSchema(cluster), nil
	case CdnInstance:
		instance := &CdnInstanceTable{}
		tx := orm.withTable(ctx, op.ResourceType).Where("instance_id = ?", id).First(instance)
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

		return tableToSchema(instance), nil
	case SecurityDomain:
		domain := &SecurityDomainTable{}
		tx := orm.withTable(ctx, op.ResourceType).Where("security_domain = ?", id).First(domain)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}

		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete security domain error: %s", tx.Error.Error())
		}

		tx = tx.Delete(domain)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}

		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete security domain error: %s", tx.Error.Error())
		}

		return tableToSchema(domain), nil
	default:
		return nil, dferrors.Newf(dfcodes.InvalidResourceType, "delete store error: not support resource type %s", op.ResourceType)
	}
}

func (orm *ormStore) Update(ctx context.Context, id string, data interface{}, opts ...OpOption) (interface{}, error) {
	op := Op{}
	op.ApplyOpts(opts)

	switch op.ResourceType {
	case SchedulerCluster:
		tCluster := &SchedulerClusterTable{}
		tx := orm.withTable(ctx, op.ResourceType).Where("cluster_id = ?", id).First(tCluster)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update scheduler cluster error: %s", err.Error())
		}

		c, ok := data.(*types.SchedulerCluster)
		if !ok {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update scheduler cluster error: reflect scheduler cluster error")
		}

		cluster := schemaToTable(c)
		updateSchemaToTable(cluster, tCluster)
		tx = tx.Updates(cluster)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update scheduler cluster error: %s", err.Error())
		} else if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update scheduler cluster error: %s", tx.Error.Error())
		} else {
			return tableToSchema(cluster), nil
		}
	case SchedulerInstance:
		tInstance := &SchedulerInstanceTable{}
		tx := orm.withTable(ctx, op.ResourceType).Where("instance_id = ?", id).First(tInstance)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update scheduler instance error: %s", err.Error())
		}

		i, ok := data.(*types.SchedulerInstance)
		if !ok {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update scheduler instance error: reflect scheduler instance error")
		}

		instance := schemaToTable(i)
		updateSchemaToTable(instance, tInstance)
		tx = tx.Updates(instance)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update scheduler instance error: %s", err.Error())
		} else if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update scheduler instance error: %s", tx.Error.Error())
		} else {
			return tableToSchema(instance), nil
		}
	case CdnCluster:
		tCluster := &CdnClusterTable{}
		tx := orm.withTable(ctx, op.ResourceType).Where("cluster_id = ?", id).First(tCluster)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update cdn cluster error: %s", err.Error())
		}

		c, ok := data.(*types.CdnCluster)
		if !ok {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update cdn cluster error: reflect cdn cluster error")
		}

		cluster := schemaToTable(c)
		updateSchemaToTable(cluster, tCluster)
		tx = tx.Updates(cluster)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update cdn cluster error: %s", err.Error())
		} else if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update cdn cluster error: %s", tx.Error.Error())
		} else {
			return tableToSchema(cluster), nil
		}
	case CdnInstance:
		tInstance := &CdnInstanceTable{}
		tx := orm.withTable(ctx, op.ResourceType).Where("instance_id = ?", id).First(tInstance)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update cdn instance error: %s", err.Error())
		}

		i, ok := data.(*types.CdnInstance)
		if !ok {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update cdn instance error: reflect cdn instance error")
		}

		instance := schemaToTable(i)
		updateSchemaToTable(instance, tInstance)
		tx = tx.Updates(instance)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update cdn instance error: %s", err.Error())
		} else if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update cdn instance error: %s", tx.Error.Error())
		} else {
			return tableToSchema(instance), nil
		}
	case SecurityDomain:
		tDomain := &SecurityDomainTable{}
		tx := orm.withTable(ctx, op.ResourceType).Where("security_domain = ?", id).First(tDomain)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update security domain error: %s", err.Error())
		}

		d, ok := data.(*types.SecurityDomain)
		if !ok {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update security domain error: reflect security domain error")
		}

		domain := schemaToTable(d)
		updateSchemaToTable(domain, tDomain)
		tx = tx.Updates(domain)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update security domain error: %s", err.Error())
		} else if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update security domain error: %s", tx.Error.Error())
		} else {
			return tableToSchema(domain), nil
		}
	default:
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update store error: not support resource type %s", op.ResourceType)
	}
}

func (orm *ormStore) Get(ctx context.Context, id string, opts ...OpOption) (interface{}, error) {
	op := Op{}
	op.ApplyOpts(opts)

	switch op.ResourceType {
	case SchedulerCluster:
		cluster := &SchedulerClusterTable{}
		tx := orm.withTable(ctx, op.ResourceType).Where("cluster_id = ?", id).First(cluster)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "get scheduler cluster error: %s", err.Error())
		} else if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "get scheduler cluster error: %s", tx.Error.Error())
		} else {
			return tableToSchema(cluster), nil
		}
	case SchedulerInstance:
		instance := &SchedulerInstanceTable{}
		tx := orm.withTable(ctx, op.ResourceType).Where("instance_id = ?", id).First(instance)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "get scheduler instance error: %s", err.Error())
		} else if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "get scheduler instance error: %s", tx.Error.Error())
		} else {
			return tableToSchema(instance), nil
		}
	case CdnCluster:
		cluster := &CdnClusterTable{}
		tx := orm.withTable(ctx, op.ResourceType).Where("cluster_id = ?", id).First(cluster)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "get cdn cluster error: %s", err.Error())
		} else if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "get cdn cluster error: %s", tx.Error.Error())
		} else {
			return tableToSchema(cluster), nil
		}
	case CdnInstance:
		instance := &CdnInstanceTable{}
		tx := orm.withTable(ctx, op.ResourceType).Where("instance_id = ?", id).First(instance)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "get cdn instance error: %s", err.Error())
		} else if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "get cdn instance error: %s", tx.Error.Error())
		} else {
			return tableToSchema(instance), nil
		}
	case SecurityDomain:
		domain := &SecurityDomainTable{}
		tx := orm.withTable(ctx, op.ResourceType).Where("security_domain = ?", id).First(domain)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "get security domain error: %s", err.Error())
		} else if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "get security domain error: %s", tx.Error.Error())
		} else {
			return tableToSchema(domain), nil
		}
	default:
		return nil, dferrors.Newf(dfcodes.InvalidResourceType, "get store error: not support resource type %s", op.ResourceType)
	}
}

func (orm *ormStore) List(ctx context.Context, opts ...OpOption) ([]interface{}, error) {
	op := Op{}
	op.ApplyOpts(opts)

	switch op.ResourceType {
	case SchedulerCluster:
		var clusters []*SchedulerClusterTable
		tx := orm.withTable(ctx, op.ResourceType).Order("cluster_id").Offset(op.Marker).Limit(op.MaxItemCount).Find(&clusters)
		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "list scheduler clusters error: %s", tx.Error.Error())
		} else {
			var inter []interface{}
			for _, cluster := range clusters {
				inter = append(inter, tableToSchema(cluster))
			}
			return inter, nil
		}
	case SchedulerInstance:
		var instances []*SchedulerInstanceTable
		var tx *gorm.DB
		if len(op.ClusterId) <= 0 {
			tx = orm.withTable(ctx, op.ResourceType).Order("instance_id").Offset(op.Marker).Limit(op.MaxItemCount).Find(&instances)
		} else {
			tx = orm.withTable(ctx, op.ResourceType).Where("cluster_id = ?", op.ClusterId).Order("instance_id").Offset(op.Marker).Limit(op.MaxItemCount).Find(&instances)
		}

		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "list scheduler instances error: %s", tx.Error.Error())
		} else {
			var inter []interface{}
			for _, instance := range instances {
				inter = append(inter, tableToSchema(instance))
			}
			return inter, nil
		}
	case CdnCluster:
		var clusters []*CdnClusterTable
		tx := orm.withTable(ctx, op.ResourceType).Order("cluster_id").Offset(op.Marker).Limit(op.MaxItemCount).Find(&clusters)
		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "list cnd clusters error %s", tx.Error.Error())
		} else {
			var inter []interface{}
			for _, cluster := range clusters {
				inter = append(inter, tableToSchema(cluster))
			}
			return inter, nil
		}
	case CdnInstance:
		var instances []*CdnInstanceTable
		var tx *gorm.DB
		if len(op.ClusterId) <= 0 {
			tx = orm.withTable(ctx, op.ResourceType).Order("instance_id").Offset(op.Marker).Limit(op.MaxItemCount).Find(&instances)
		} else {
			tx = orm.withTable(ctx, op.ResourceType).Where("cluster_id = ?", op.ClusterId).Order("instance_id").Offset(op.Marker).Limit(op.MaxItemCount).Find(&instances)
		}

		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "list cdn instances error %s", tx.Error.Error())
		} else {
			var inter []interface{}
			for _, instance := range instances {
				inter = append(inter, tableToSchema(instance))
			}
			return inter, nil
		}
	case SecurityDomain:
		var domains []*SecurityDomainTable
		var tx *gorm.DB
		tx = orm.withTable(ctx, op.ResourceType).Order("security_domain").Offset(op.Marker).Limit(op.MaxItemCount).Find(&domains)

		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "list security domain error %s", tx.Error.Error())
		} else {
			var inter []interface{}
			for _, domain := range domains {
				inter = append(inter, tableToSchema(domain))
			}
			return inter, nil
		}
	default:
		return nil, dferrors.Newf(dfcodes.InvalidResourceType, "list store error, not support resource type %s", op.ResourceType)
	}
}
