package host

import "context"

type defaultHostManager struct {
}

func (empty *defaultHostManager) GetHostInfo(ctx context.Context, opts ...OpOption) (*Info, error) {
	op := Op{}
	op.ApplyOpts(opts)

	return NewDefaultHostInfo(op.ip, op.hostName), nil
}

func NewDefaultHostManager() (Manager, error) {
	return &defaultHostManager{}, nil
}
