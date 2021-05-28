package dc

import (
	"context"
	"fmt"
	"time"

	"d7y.io/dragonfly/v2/manager/config"
	"github.com/go-redis/redis/v8"
)

type RedisClient struct {
	Client        *redis.Client
	ClusterClient *redis.ClusterClient
}

func NewRedisClient(cfg *config.RedisConfig) (*RedisClient, error) {
	if err := cfg.Valid(); err != nil {
		return nil, err
	}

	client := &RedisClient{}
	if len(cfg.Addrs) == 1 {
		client.Client = redis.NewClient(&redis.Options{
			Username: cfg.User,
			Password: cfg.Password,
			Addr:     cfg.Addrs[0],
		})
	} else {
		client.ClusterClient = redis.NewClusterClient(&redis.ClusterOptions{
			Username: cfg.User,
			Password: cfg.Password,
			Addrs:    cfg.Addrs,
		})
	}

	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()

	err := client.process(ctx, redis.NewStringCmd(ctx, "ping"))
	if err != nil {
		client.Close()
		return nil, err
	}

	return client, nil
}

func (client *RedisClient) process(ctx context.Context, cmd redis.Cmder) error {
	if client.Client != nil {
		return client.Client.Process(ctx, cmd)
	}

	if client.ClusterClient != nil {
		return client.ClusterClient.Process(ctx, cmd)
	}

	return fmt.Errorf("client and clusterClient are both nil")
}

func (client *RedisClient) Close() error {
	if client.Client != nil {
		return client.Client.Close()
	}

	if client.ClusterClient != nil {
		return client.ClusterClient.Close()
	}

	return nil
}
