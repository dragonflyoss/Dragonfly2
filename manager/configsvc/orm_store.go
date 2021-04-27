package configsvc

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
	ID              uint   `gorm:"primaryKey"`
	ClusterId       string `gorm:"unique;size:63"`
	SchedulerConfig string `gorm:"size:4095"`
	ClientConfig    string `gorm:"size:4095"`
	Creator         string `gorm:"size:31"`
	Modifier        string `gorm:"size:31"`
	CreatedAt       time.Time
	UpdatedAt       time.Time
	DeletedAt       gorm.DeletedAt `gorm:"index"`
}

type SchedulerInstanceTable struct {
	ID             uint   `gorm:"primaryKey"`
	InstanceId     string `gorm:"unique;size:63"`
	ClusterId      string `gorm:"size:63"`
	SecurityDomain string `gorm:"size:63"`
	Vips           string `gorm:"size:4095"`
	Idc            string `gorm:"size:63"`
	Location       string `gorm:"size:4095"`
	NetConfig      string `gorm:"size:4095"`
	HostName       string `gorm:"size:63"`
	Ip             string `gorm:"size:31"`
	Port           int32  `gorm:"port"`
	State          string `gorm:"size:15"`
	CreatedAt      time.Time
	UpdatedAt      time.Time
	DeletedAt      gorm.DeletedAt `gorm:"index"`
}

type CdnClusterTable struct {
	ID        uint   `gorm:"primaryKey"`
	ClusterId string `gorm:"unique;size:63"`
	Config    string `gorm:"size:4095"`
	Creator   string `gorm:"size:31"`
	Modifier  string `gorm:"size:31"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`
}

type CdnInstanceTable struct {
	ID         uint   `gorm:"primaryKey"`
	InstanceId string `gorm:"unique;size:63"`
	ClusterId  string `gorm:"size:63"`
	Idc        string `gorm:"size:63"`
	Location   string `gorm:"size:4095"`
	HostName   string `gorm:"size:63"`
	Ip         string `gorm:"size:31"`
	Port       int32  `gorm:"port"`
	RpcPort    int32  `gorm:"rpc_port"`
	DownPort   int32  `gorm:"down_port"`
	State      string `gorm:"size:15"`
	CreatedAt  time.Time
	UpdatedAt  time.Time
	DeletedAt  gorm.DeletedAt `gorm:"index"`
}

type SecurityDomainTable struct {
	ID             uint   `gorm:"primaryKey"`
	SecurityDomain string `gorm:"unique;size:63"`
	DisplayName    string `gorm:"size:63"`
	ProxyDomain    string `gorm:"size:4095"`
	Creator        string `gorm:"size:31"`
	Modifier       string `gorm:"size:31"`
	CreatedAt      time.Time
	UpdatedAt      time.Time
	DeletedAt      gorm.DeletedAt `gorm:"index"`
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

		if err := orm.withTable(SchedulerCluster).AutoMigrate(&SchedulerClusterTable{}); err != nil {
			return nil, err
		}

		if err := orm.withTable(SchedulerInstance).AutoMigrate(&SchedulerInstanceTable{}); err != nil {
			return nil, err
		}

		if err := orm.withTable(CdnCluster).AutoMigrate(&CdnClusterTable{}); err != nil {
			return nil, err
		}

		if err := orm.withTable(CdnInstance).AutoMigrate(&CdnInstanceTable{}); err != nil {
			return nil, err
		}

		if err := orm.withTable(SecurityDomain).AutoMigrate(&SecurityDomainTable{}); err != nil {
			return nil, err
		}

		return orm, nil
	}
}

func (orm *ormStore) withTable(resourceType ResourceType) (tx *gorm.DB) {
	switch resourceType {
	case SchedulerCluster, SchedulerInstance, CdnCluster, CdnInstance, SecurityDomain:
		return orm.db.Table(resourceType.String())
	default:
		return orm.db
	}
}

func schemaToTable(data interface{}, ID uint) interface{} {
	switch t := data.(type) {
	case *types.SchedulerCluster:
		return &SchedulerClusterTable{
			ID:              ID,
			ClusterId:       t.ClusterId,
			SchedulerConfig: t.SchedulerConfig,
			ClientConfig:    t.ClientConfig,
			Creator:         t.Creator,
			Modifier:        t.Modifier,
		}
	case *types.SchedulerInstance:
		return &SchedulerInstanceTable{
			ID:             ID,
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
		}
	case *types.CdnCluster:
		return &CdnClusterTable{
			ID:        ID,
			ClusterId: t.ClusterId,
			Config:    t.Config,
			Creator:   t.Creator,
			Modifier:  t.Modifier,
		}
	case *types.CdnInstance:
		return &CdnInstanceTable{
			ID:         ID,
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
		}
	case *types.SecurityDomain:
		return &SecurityDomainTable{
			ID:             ID,
			SecurityDomain: t.SecurityDomain,
			DisplayName:    t.DisplayName,
			ProxyDomain:    t.ProxyDomain,
			Creator:        t.Creator,
			Modifier:       t.Modifier,
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
			CreatedAt:       t.CreatedAt,
			UpdatedAt:       t.UpdatedAt,
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
			CreatedAt:      t.CreatedAt,
			UpdatedAt:      t.UpdatedAt,
		}
	case *CdnClusterTable:
		return &types.CdnCluster{
			ClusterId: t.ClusterId,
			Config:    t.Config,
			Creator:   t.Creator,
			Modifier:  t.Modifier,
			CreatedAt: t.CreatedAt,
			UpdatedAt: t.UpdatedAt,
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
			CreatedAt:  t.CreatedAt,
			UpdatedAt:  t.UpdatedAt,
		}
	case *SecurityDomainTable:
		return &types.SecurityDomain{
			SecurityDomain: t.SecurityDomain,
			DisplayName:    t.DisplayName,
			ProxyDomain:    t.ProxyDomain,
			Creator:        t.Creator,
			Modifier:       t.Modifier,
			CreatedAt:      t.CreatedAt,
			UpdatedAt:      t.UpdatedAt,
		}
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

		cluster := schemaToTable(c, 0)
		tx := orm.withTable(op.ResourceType).Create(cluster)
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

		instance := schemaToTable(i, 0)
		tx := orm.withTable(op.ResourceType).Create(instance)
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

		cluster := schemaToTable(c, 0)
		tx := orm.withTable(op.ResourceType).Create(cluster)
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

		instance := schemaToTable(i, 0)
		tx := orm.withTable(op.ResourceType).Create(instance)
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

		domain := schemaToTable(d, 0)
		tx := orm.withTable(op.ResourceType).Create(domain)
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
		tx := orm.withTable(op.ResourceType).Where("cluster_id = ?", id).First(cluster)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}

		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete scheduler cluster error: %s", tx.Error.Error())
		}

		tx = orm.withTable(op.ResourceType).Delete(cluster)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}

		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete scheduler cluster error: %s", tx.Error.Error())
		}

		return tableToSchema(cluster), nil
	case SchedulerInstance:
		instance := &SchedulerInstanceTable{}
		tx := orm.withTable(op.ResourceType).Where("instance_id = ?", id).First(instance)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}

		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete scheduler instance error: %s", tx.Error.Error())
		}

		tx = orm.withTable(op.ResourceType).Delete(instance)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}

		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete scheduler instance error: %s", tx.Error.Error())
		}

		return tableToSchema(instance), nil
	case CdnCluster:
		cluster := &CdnClusterTable{}
		tx := orm.withTable(op.ResourceType).Where("cluster_id = ?", id).First(cluster)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}

		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete cdn cluster error: %s", tx.Error.Error())
		}

		tx = orm.withTable(op.ResourceType).Delete(cluster)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}

		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete cdn cluster error: %s", tx.Error.Error())
		}

		return tableToSchema(cluster), nil
	case CdnInstance:
		instance := &CdnInstanceTable{}
		tx := orm.withTable(op.ResourceType).Where("instance_id = ?", id).First(instance)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}

		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete cdn instance error: %s", tx.Error.Error())
		}

		tx = orm.withTable(op.ResourceType).Delete(instance)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}

		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete cdn instance error: %s", tx.Error.Error())
		}

		return tableToSchema(instance), nil
	case SecurityDomain:
		domain := &SecurityDomainTable{}
		tx := orm.withTable(op.ResourceType).Where("security_domain = ?", id).First(domain)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}

		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete security domain error: %s", tx.Error.Error())
		}

		tx = orm.withTable(op.ResourceType).Delete(domain)
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
		tx := orm.withTable(op.ResourceType).Where("cluster_id = ?", id).First(tCluster)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update scheduler cluster error: %s", err.Error())
		}

		c, ok := data.(*types.SchedulerCluster)
		if !ok {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update scheduler cluster error: reflect scheduler cluster error")
		}

		cluster := schemaToTable(c, tCluster.ID)
		tx = orm.withTable(op.ResourceType).Updates(cluster)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update scheduler cluster error: %s", err.Error())
		} else if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update scheduler cluster error: %s", tx.Error.Error())
		} else {
			return tableToSchema(cluster), nil
		}
	case SchedulerInstance:
		tInstance := &SchedulerInstanceTable{}
		tx := orm.withTable(op.ResourceType).Where("instance_id = ?", id).First(tInstance)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update scheduler instance error: %s", err.Error())
		}

		i, ok := data.(*types.SchedulerInstance)
		if !ok {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update scheduler instance error: reflect scheduler instance error")
		}

		instance := schemaToTable(i, tInstance.ID)
		tx = orm.withTable(op.ResourceType).Updates(instance)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update scheduler instance error: %s", err.Error())
		} else if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update scheduler instance error: %s", tx.Error.Error())
		} else {
			return tableToSchema(instance), nil
		}
	case CdnCluster:
		tCluster := &CdnClusterTable{}
		tx := orm.withTable(op.ResourceType).Where("cluster_id = ?", id).First(tCluster)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update cdn cluster error: %s", err.Error())
		}

		c, ok := data.(*types.CdnCluster)
		if !ok {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update cdn cluster error: reflect cdn cluster error")
		}

		cluster := schemaToTable(c, tCluster.ID)
		tx = orm.withTable(op.ResourceType).Updates(cluster)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update cdn cluster error: %s", err.Error())
		} else if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update cdn cluster error: %s", tx.Error.Error())
		} else {
			return tableToSchema(cluster), nil
		}
	case CdnInstance:
		tInstance := &CdnInstanceTable{}
		tx := orm.withTable(op.ResourceType).Where("instance_id = ?", id).First(tInstance)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update cdn instance error: %s", err.Error())
		}

		i, ok := data.(*types.CdnInstance)
		if !ok {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update cdn instance error: reflect cdn instance error")
		}

		instance := schemaToTable(i, tInstance.ID)
		tx = orm.withTable(op.ResourceType).Updates(instance)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update cdn instance error: %s", err.Error())
		} else if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update cdn instance error: %s", tx.Error.Error())
		} else {
			return tableToSchema(instance), nil
		}
	case SecurityDomain:
		tDomain := &SecurityDomainTable{}
		tx := orm.withTable(op.ResourceType).Where("security_domain = ?", id).First(tDomain)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update security domain error: %s", err.Error())
		}

		i, ok := data.(*types.SecurityDomain)
		if !ok {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update security domain error: reflect security domain error")
		}

		domain := schemaToTable(i, tDomain.ID)
		tx = orm.withTable(op.ResourceType).Updates(domain)
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
		tx := orm.withTable(op.ResourceType).Where("cluster_id = ?", id).First(cluster)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "get scheduler cluster error: %s", err.Error())
		} else if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "get scheduler cluster error: %s", tx.Error.Error())
		} else {
			return tableToSchema(cluster), nil
		}
	case SchedulerInstance:
		instance := &SchedulerInstanceTable{}
		tx := orm.withTable(op.ResourceType).Where("instance_id = ?", id).First(instance)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "get scheduler instance error: %s", err.Error())
		} else if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "get scheduler instance error: %s", tx.Error.Error())
		} else {
			return tableToSchema(instance), nil
		}
	case CdnCluster:
		cluster := &CdnClusterTable{}
		tx := orm.withTable(op.ResourceType).Where("cluster_id = ?", id).First(cluster)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "get cdn cluster error: %s", err.Error())
		} else if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "get cdn cluster error: %s", tx.Error.Error())
		} else {
			return tableToSchema(cluster), nil
		}
	case CdnInstance:
		instance := &CdnInstanceTable{}
		tx := orm.withTable(op.ResourceType).Where("instance_id = ?", id).First(instance)
		if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "get cdn instance error: %s", err.Error())
		} else if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "get cdn instance error: %s", tx.Error.Error())
		} else {
			return tableToSchema(instance), nil
		}
	case SecurityDomain:
		domain := &SecurityDomainTable{}
		tx := orm.withTable(op.ResourceType).Where("security_domain = ?", id).First(domain)
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
		tx := orm.withTable(op.ResourceType).Order("cluster_id").Offset(op.Marker).Limit(op.MaxItemCount).Find(&clusters)
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
			tx = orm.withTable(op.ResourceType).Order("instance_id").Offset(op.Marker).Limit(op.MaxItemCount).Find(&instances)
		} else {
			tx = orm.withTable(op.ResourceType).Where("cluster_id = ?", op.ClusterId).Order("instance_id").Offset(op.Marker).Limit(op.MaxItemCount).Find(&instances)
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
		tx := orm.withTable(op.ResourceType).Order("cluster_id").Offset(op.Marker).Limit(op.MaxItemCount).Find(&clusters)
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
			tx = orm.withTable(op.ResourceType).Order("instance_id").Offset(op.Marker).Limit(op.MaxItemCount).Find(&instances)
		} else {
			tx = orm.withTable(op.ResourceType).Where("cluster_id = ?", op.ClusterId).Order("instance_id").Offset(op.Marker).Limit(op.MaxItemCount).Find(&instances)
		}

		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "list cnd instances error %s", tx.Error.Error())
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
		tx = orm.withTable(op.ResourceType).Order("security_domain").Offset(op.Marker).Limit(op.MaxItemCount).Find(&domains)

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
