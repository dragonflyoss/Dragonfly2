package store

import (
	"context"
)

type ResourceType string

const (
	SchedulerCluster  ResourceType = "SchedulerCluster"
	SchedulerInstance ResourceType = "SchedulerInstance"
	CDNCluster        ResourceType = "CDNCluster"
	CDNInstance       ResourceType = "CDNInstance"
	SecurityDomain    ResourceType = "SecurityDomain"
)

func (objType ResourceType) String() string {
	return string(objType)
}

type Op struct {
	ResourceType ResourceType
	ClusterID    string
	InstanceID   string
	Marker       int
	MaxItemCount int
}

type OpOption func(*Op)

type Store interface {
	Add(ctx context.Context, id string, data interface{}, opts ...OpOption) (interface{}, error)
	Delete(ctx context.Context, id string, opts ...OpOption) (interface{}, error)
	Update(ctx context.Context, id string, data interface{}, opts ...OpOption) (interface{}, error)
	Get(ctx context.Context, id string, opts ...OpOption) (interface{}, error)
	List(ctx context.Context, opts ...OpOption) ([]interface{}, error)
}

func (op *Op) ApplyOpts(opts []OpOption) {
	for _, opt := range opts {
		opt(op)
	}
}

func WithResourceType(objType ResourceType) OpOption {
	return func(op *Op) {
		op.ResourceType = objType
	}
}

func WithClusterID(clusterID string) OpOption {
	return func(op *Op) {
		op.ClusterID = clusterID
	}
}

func WithInstanceID(instanceID string) OpOption {
	return func(op *Op) {
		op.InstanceID = instanceID
	}
}

func WithMarker(marker, maxItemCount int) OpOption {
	return func(op *Op) {
		op.Marker = marker
		op.MaxItemCount = maxItemCount
	}
}
