package client

import (
	"context"
	"time"

	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

const (
	contextTimeout    = 2 * time.Minute
	backoffBaseDelay  = 1 * time.Second
	backoffMultiplier = 1.6
	backoffJitter     = 0.2
	backoffMaxDelay   = 10 * time.Second
)

type Client interface {
	Close() error
	GetScheduler(*manager.GetSchedulerRequest) (*manager.Scheduler, error)
	UpdateScheduler(*manager.UpdateSchedulerRequest) (*manager.Scheduler, error)
	UpdateCDN(*manager.UpdateCDNRequest) (*manager.CDN, error)
	KeepAlive(time.Duration, *manager.KeepAliveRequest)
}

type client struct {
	manager.ManagerClient
	conn *grpc.ClientConn
}

func New(target string) (Client, error) {
	conn, err := grpc.Dial(
		target,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  backoffBaseDelay,
				Multiplier: backoffMultiplier,
				Jitter:     backoffJitter,
				MaxDelay:   backoffMaxDelay,
			},
		}),
	)
	if err != nil {
		return nil, err
	}

	return &client{
		ManagerClient: manager.NewManagerClient(conn),
		conn:          conn,
	}, nil
}

func (c *client) GetScheduler(scheduler *manager.GetSchedulerRequest) (*manager.Scheduler, error) {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	return c.ManagerClient.GetScheduler(ctx, scheduler)
}

func (c *client) UpdateScheduler(scheduler *manager.UpdateSchedulerRequest) (*manager.Scheduler, error) {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	return c.ManagerClient.UpdateScheduler(ctx, scheduler)
}

func (c *client) UpdateCDN(cdn *manager.UpdateCDNRequest) (*manager.CDN, error) {
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()

	return c.ManagerClient.UpdateCDN(ctx, cdn)
}

func (c *client) KeepAlive(interval time.Duration, keepalive *manager.KeepAliveRequest) {
retry:
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := c.ManagerClient.KeepAlive(ctx)
	if err != nil {
		time.Sleep(interval)
		cancel()
		goto retry
	}

	tick := time.NewTicker(interval)
	for {
		select {
		case <-tick.C:
			if err := stream.Send(&manager.KeepAliveRequest{
				HostName:   keepalive.HostName,
				SourceType: keepalive.SourceType,
				ClusterId:  keepalive.ClusterId,
			}); err != nil {
				stream.CloseAndRecv()
				cancel()
				goto retry
			}
		}
	}
}

func (c *client) Close() error {
	return c.conn.Close()
}
