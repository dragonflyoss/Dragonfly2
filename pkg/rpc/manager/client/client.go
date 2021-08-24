package client

import (
	"context"
	"time"

	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"google.golang.org/grpc"
)

const (
	contextTimeout = 2 * time.Minute
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
