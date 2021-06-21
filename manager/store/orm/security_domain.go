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

type SecurityDomainTable struct {
	ID             uint           `gorm:"column:id;primaryKey"`
	SecurityDomain string         `gorm:"column:security_domain;unique;size:63"`
	DisplayName    string         `gorm:"column:display_name;size:63"`
	ProxyDomain    string         `gorm:"column:proxy_domain;type:text;not null"`
	Config         string         `gorm:"column:config;type:text;not null"`
	Creator        string         `gorm:"column:creator;size:31"`
	Modifier       string         `gorm:"column:modifier;size:31"`
	Version        int64          `gorm:"column:version"`
	Description    string         `gorm:"column:description;size:1023"`
	CreatedAt      time.Time      `gorm:"column:created_at"`
	UpdatedAt      time.Time      `gorm:"column:updated_at"`
	DeletedAt      gorm.DeletedAt `gorm:"column:deleted_at;index"`
}

type SecurityDomainStore struct {
	resourceType store.ResourceType
	db           *gorm.DB
	table        string
}

var _ store.Store = (*SecurityDomainStore)(nil)

func NewSecurityDomainStore(db *gorm.DB, table string) (store.Store, error) {
	s := &SecurityDomainStore{
		resourceType: store.SecurityDomain,
		db:           db,
		table:        table,
	}

	err := s.withTable(context.Background()).AutoMigrate(&SecurityDomainTable{})
	if err != nil {
		return nil, err
	}

	return s, nil
}

func SecurityDomainToTable(t *types.SecurityDomain) *SecurityDomainTable {
	return &SecurityDomainTable{
		SecurityDomain: t.SecurityDomain,
		DisplayName:    t.DisplayName,
		ProxyDomain:    t.ProxyDomain,
		Creator:        t.Creator,
		Modifier:       t.Modifier,
		Version:        time.Now().UnixNano(),
	}
}

func SecurityDomainToSchema(t *SecurityDomainTable) *types.SecurityDomain {
	return &types.SecurityDomain{
		SecurityDomain: t.SecurityDomain,
		DisplayName:    t.DisplayName,
		ProxyDomain:    t.ProxyDomain,
		Creator:        t.Creator,
		Modifier:       t.Modifier,
		CreatedAt:      t.CreatedAt.String(),
		UpdatedAt:      t.UpdatedAt.String(),
	}
}

func (s *SecurityDomainStore) updateSchemaToTable(new, old *SecurityDomainTable) *SecurityDomainTable {
	new.ID = old.ID
	if new.ProxyDomain != old.ProxyDomain || new.DisplayName != old.DisplayName {
		new.Version = time.Now().UnixNano()
	} else {
		new.Version = old.Version
	}
	return new
}

func (s *SecurityDomainStore) withTable(ctx context.Context) (tx *gorm.DB) {
	return s.db.WithContext(ctx).Table(s.table)
}

func (s *SecurityDomainStore) Add(ctx context.Context, id string, data interface{}, opts ...store.OpOption) (interface{}, error) {
	d, ok := data.(*types.SecurityDomain)
	if !ok {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add security domain error: reflect security domain error")
	}

	domain := SecurityDomainToTable(d)
	tx := s.withTable(ctx).Create(domain)
	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add security domain error: %s", tx.Error.Error())
	}

	return SecurityDomainToSchema(domain), nil
}

func (s *SecurityDomainStore) Delete(ctx context.Context, id string, opts ...store.OpOption) (interface{}, error) {
	domain := &SecurityDomainTable{}
	tx := s.withTable(ctx).Where("security_domain = ?", id).First(domain)
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

	return SecurityDomainToSchema(domain), nil
}

func (s *SecurityDomainStore) Update(ctx context.Context, id string, data interface{}, opts ...store.OpOption) (interface{}, error) {
	tDomain := &SecurityDomainTable{}
	tx := s.withTable(ctx).Where("security_domain = ?", id).First(tDomain)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update security domain error: %s", err.Error())
	}

	d, ok := data.(*types.SecurityDomain)
	if !ok {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update security domain error: reflect security domain error")
	}

	domain := SecurityDomainToTable(d)
	s.updateSchemaToTable(domain, tDomain)
	tx = tx.Updates(domain)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update security domain error: %s", err.Error())
	} else if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update security domain error: %s", tx.Error.Error())
	} else {
		return SecurityDomainToSchema(domain), nil
	}
}

func (s *SecurityDomainStore) Get(ctx context.Context, id string, opts ...store.OpOption) (interface{}, error) {
	domain := &SecurityDomainTable{}
	tx := s.withTable(ctx).Where("security_domain = ?", id).First(domain)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "get security domain error: %s", err.Error())
	} else if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "get security domain error: %s", tx.Error.Error())
	} else {
		return SecurityDomainToSchema(domain), nil
	}
}

func (s *SecurityDomainStore) List(ctx context.Context, opts ...store.OpOption) ([]interface{}, error) {
	op := store.Op{}
	op.ApplyOpts(opts)

	var domains []*SecurityDomainTable
	var tx *gorm.DB
	tx = s.withTable(ctx).Order("security_domain").Offset(op.Marker).Limit(op.MaxItemCount).Find(&domains)

	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "list security domain error %s", tx.Error.Error())
	}

	var inter []interface{}
	for _, domain := range domains {
		inter = append(inter, SecurityDomainToSchema(domain))
	}

	return inter, nil
}
