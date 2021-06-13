package orm

import (
	"context"
	"errors"
	"time"

	"d7y.io/dragonfly/v2/manager/lease"
	"d7y.io/dragonfly/v2/manager/store"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"gorm.io/gorm"
)

type LeaseTable struct {
	ID        uint      `gorm:"column:id;primaryKey"`
	LeaseID   string    `gorm:"column:lease_id;unique;size:63"`
	Key       string    `gorm:"column:lease_key;unique;size:255"`
	Value     string    `gorm:"column:lease_value;size:255"`
	TTL       int64     `gorm:"column:ttl"`
	CreatedAt time.Time `gorm:"column:created_at"`
	UpdatedAt time.Time `gorm:"column:updated_at"`
}

type LeaseStore struct {
	resourceType store.ResourceType
	db           *gorm.DB
	table        string
}

var _ store.Store = (*LeaseStore)(nil)

func NewLeaseStore(db *gorm.DB, table string) (store.Store, error) {
	s := &LeaseStore{
		resourceType: store.Lease,
		db:           db,
		table:        table,
	}

	err := s.withTable(context.Background()).AutoMigrate(&LeaseTable{})
	if err != nil {
		return nil, err
	}

	return s, nil
}

func LeaseToTable(t *lease.Lease) *LeaseTable {
	return &LeaseTable{
		LeaseID: string(t.LeaseID),
		Key:     t.Key,
		Value:   t.Value,
		TTL:     t.TTL,
	}
}

func LeaseToSchema(t *LeaseTable) *lease.Lease {
	return &lease.Lease{
		LeaseID: lease.LeaseID(t.LeaseID),
		Key:     t.Key,
		Value:   t.Value,
		TTL:     t.TTL,
	}
}

func (s *LeaseStore) updateSchemaToTable(new, old *LeaseTable) *LeaseTable {
	new.ID = old.ID
	new.LeaseID = old.LeaseID
	new.Key = old.Key
	new.Value = old.Value
	new.TTL = old.TTL
	return new
}

func (s *LeaseStore) withTable(ctx context.Context) (tx *gorm.DB) {
	return s.db.WithContext(ctx).Table(s.table)
}

func (s *LeaseStore) Add(ctx context.Context, id string, data interface{}, opts ...store.OpOption) (interface{}, error) {
	l, ok := data.(*lease.Lease)
	if !ok {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add lease error: reflect lease error")
	}

	le := LeaseToTable(l)
	tx := s.withTable(ctx).Create(le)

	if tx.Error != nil {
		err := tx.Error

		tLease := &LeaseTable{}
		tx = s.withTable(ctx).Where("lease_key = ?", le.Key).First(tLease)
		if tx.Error != nil {
			return nil, dferrors.Newf(dfcodes.ManagerStoreError, "add lease error: %s", tx.Error.Error())
		}

		now := time.Now()
		if now.Before(tLease.UpdatedAt.Add(time.Duration(tLease.TTL) * time.Second)) {
			return nil, err
		}

		tx = tx.Unscoped().Delete(tLease)
		if tx.Error != nil {
			return nil, tx.Error
		}

		tx = tx.Create(le)
		if tx.Error != nil {
			return nil, tx.Error
		}
	}

	return LeaseToSchema(le), nil
}

func (s *LeaseStore) Delete(ctx context.Context, id string, opts ...store.OpOption) (interface{}, error) {
	le := &LeaseTable{}
	tx := s.withTable(ctx).Where("lease_id = ?", id).First(le)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}

	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete lease error: %s", tx.Error.Error())
	}

	tx = tx.Unscoped().Delete(le)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}

	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "delete lease error: %s", tx.Error.Error())
	}

	return LeaseToSchema(le), nil
}

func (s *LeaseStore) Update(ctx context.Context, id string, data interface{}, opts ...store.OpOption) (interface{}, error) {
	tLease := &LeaseTable{}
	tx := s.withTable(ctx).Where("lease_id = ?", id).First(tLease)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update lease error: %s", err.Error())
	}

	l, ok := data.(*lease.Lease)
	if !ok {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update lease error: reflect lease error")
	}

	le := LeaseToTable(l)
	s.updateSchemaToTable(le, tLease)
	tx = tx.Updates(le)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "update lease error: %s", err.Error())
	} else if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "update lease error: %s", tx.Error.Error())
	} else {
		return LeaseToSchema(le), nil
	}
}

func (s *LeaseStore) Get(ctx context.Context, id string, opts ...store.OpOption) (interface{}, error) {
	le := &LeaseTable{}
	tx := s.withTable(ctx).Where("lease_id = ?", id).First(le)
	if err := tx.Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dferrors.Newf(dfcodes.ManagerStoreNotFound, "get lease error: %s", err.Error())
	} else if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "get lease error: %s", tx.Error.Error())
	} else {
		return LeaseToSchema(le), nil
	}
}

func (s *LeaseStore) List(ctx context.Context, opts ...store.OpOption) ([]interface{}, error) {
	op := store.Op{}
	op.ApplyOpts(opts)

	var leases []*LeaseTable
	var tx *gorm.DB
	tx = s.withTable(ctx).Order("lease_id").Offset(op.Marker).Limit(op.MaxItemCount).Find(&leases)

	if tx.Error != nil {
		return nil, dferrors.Newf(dfcodes.ManagerStoreError, "list security domain error %s", tx.Error.Error())
	}

	var inter []interface{}
	for _, le := range leases {
		inter = append(inter, LeaseToSchema(le))
	}

	return inter, nil
}
