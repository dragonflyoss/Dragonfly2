package rpcserver

import (
	"d7y.io/dragonfly/v2/pkg/rpc/trainer/server"
	"d7y.io/dragonfly/v2/trainer/config"
	"google.golang.org/grpc"
)

// TODO(fyx) Add config, storage, service, resource and metrics when Initialize new trainer server.
// New returns a new trainer server from the given options.
func New(
	cfg *config.Config,
	opts ...grpc.ServerOption,
) *grpc.Server {
	return server.New(
		newTrainerServerV1(cfg),
		opts...)
}
