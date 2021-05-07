package host

import "context"

type defaultHostManager struct {
}

func (empty *defaultHostManager) GetHostInfo(ctx context.Context, opts ...OpOption) (*HostInfo, error) {
	op := Op{}
	op.ApplyOpts(opts)

	return NewDefaultHostInfo(op.ip, op.hostName), nil
}

func NewDefaultHostManager() (HostManager, error) {
	return &defaultHostManager{}, nil
}
