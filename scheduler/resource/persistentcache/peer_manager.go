/*
 *     Copyright 2024 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//go:generate mockgen -destination peer_manager_mock.go -source peer_manager.go -package persistentcache

package persistentcache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/bits-and-blooms/bitset"
	redis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	dfdaemonv2 "d7y.io/api/v2/pkg/apis/dfdaemon/v2"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
	dfdaemonclient "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	"d7y.io/dragonfly/v2/scheduler/config"
)

// PeerManager is the interface used for peer manager.
type PeerManager interface {
	// Load returns peer by a key.
	Load(context.Context, string) (*Peer, bool)

	// Store sets peer.
	Store(context.Context, *Peer) error

	// Delete deletes peer by a key.
	Delete(context.Context, string) error

	// LoadAll returns all peers.
	LoadAll(context.Context) ([]*Peer, error)

	// LoadAllByTaskID returns all peers by task id.
	LoadAllByTaskID(context.Context, string) ([]*Peer, error)

	// DeleteAllByTaskID deletes all peers by task id.
	DeleteAllByTaskID(context.Context, string) error

	// LoadAllByHostID returns all peers by host id.
	LoadAllByHostID(context.Context, string) ([]*Peer, error)

	// DeleteAllByHostID deletes all peers by host id.
	DeleteAllByHostID(context.Context, string) error
}

// peerManager contains content for peer manager.
type peerManager struct {
	// Config is scheduler config.
	config *config.Config

	// taskManager is the manager of task.
	taskManager TaskManager

	// hostManager is the manager of host.
	hostManager HostManager

	// Redis universal client interface.
	rdb redis.UniversalClient

	// transportCredentials is used to mTLS for peer grpc connection.
	transportCredentials credentials.TransportCredentials
}

// New peer manager interface.
func newPeerManager(cfg *config.Config, rdb redis.UniversalClient, taskManager TaskManager, hostManager HostManager, transportCredentials credentials.TransportCredentials) PeerManager {
	return &peerManager{config: cfg, rdb: rdb, taskManager: taskManager, hostManager: hostManager, transportCredentials: transportCredentials}
}

// Load returns persistent cache peer by a key.
func (p *peerManager) Load(ctx context.Context, peerID string) (*Peer, bool) {
	log := logger.WithPeerID(peerID)
	rawPeer, err := p.rdb.HGetAll(ctx, pkgredis.MakePersistentCachePeerKeyInScheduler(p.config.Manager.SchedulerClusterID, peerID)).Result()
	if err != nil {
		log.Errorf("getting peer failed from redis: %v", err)
		return nil, false
	}

	if len(rawPeer) == 0 {
		return nil, false
	}

	persistent, err := strconv.ParseBool(rawPeer["persistent"])
	if err != nil {
		log.Errorf("parsing persistent failed: %v", err)
		return nil, false
	}

	finishedPieces := &bitset.BitSet{}
	if err := finishedPieces.UnmarshalBinary([]byte(rawPeer["finished_pieces"])); err != nil {
		log.Errorf("unmarshal finished pieces failed: %v", err)
		return nil, false
	}

	blockParents := []string{}
	if err := json.Unmarshal([]byte(rawPeer["block_parents"]), &blockParents); err != nil {
		log.Errorf("unmarshal block parents failed: %v", err)
		return nil, false
	}

	// Set time fields from raw task.
	cost, err := strconv.ParseUint(rawPeer["cost"], 10, 64)
	if err != nil {
		log.Errorf("parsing cost failed: %v", err)
		return nil, false
	}

	createdAt, err := time.Parse(time.RFC3339, rawPeer["created_at"])
	if err != nil {
		log.Errorf("parsing created at failed: %v", err)
		return nil, false
	}

	updatedAt, err := time.Parse(time.RFC3339, rawPeer["updated_at"])
	if err != nil {
		log.Errorf("parsing updated at failed: %v", err)
		return nil, false
	}

	host, loaded := p.hostManager.Load(ctx, rawPeer["host_id"])
	if !loaded {
		log.Errorf("host not found")
		return nil, false
	}

	task, loaded := p.taskManager.Load(ctx, rawPeer["task_id"])
	if !loaded {
		log.Errorf("task not found")
		return nil, false
	}

	return NewPeer(
		rawPeer["id"],
		rawPeer["state"],
		persistent,
		finishedPieces,
		blockParents,
		task,
		host,
		time.Duration(cost),
		createdAt,
		updatedAt,
		logger.WithPeer(host.ID, task.ID, rawPeer["id"]),
	), true
}

// Store sets persistent cache peer.
func (p *peerManager) Store(ctx context.Context, peer *Peer) error {
	finishedPieces, err := peer.FinishedPieces.MarshalBinary()
	if err != nil {
		peer.Log.Errorf("marshal finished pieces failed: %v", err)
		return err
	}

	blockParents, err := json.Marshal(peer.BlockParents)
	if err != nil {
		peer.Log.Errorf("marshal block parents failed: %v", err)
		return err
	}

	if _, err := p.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		// Store peer information and set expiration.
		if _, err := pipe.HSet(ctx,
			pkgredis.MakePersistentCachePeerKeyInScheduler(p.config.Manager.SchedulerClusterID, peer.ID),
			"id", peer.ID,
			"persistent", peer.Persistent,
			"finished_pieces", finishedPieces,
			"state", peer.FSM.Current(),
			"block_parents", blockParents,
			"task_id", peer.Task.ID,
			"host_id", peer.Host.ID,
			"ttl", peer.Cost,
			"created_at", peer.CreatedAt.Format(time.RFC3339),
			"updated_at", peer.UpdatedAt.Format(time.RFC3339)).Result(); err != nil {
			peer.Log.Errorf("store peer failed: %v", err)
			return err
		}

		ttl := peer.Task.TTL - time.Since(peer.Task.CreatedAt)
		if _, err := pipe.Expire(ctx, pkgredis.MakePersistentCachePeerKeyInScheduler(p.config.Manager.SchedulerClusterID, peer.ID), ttl).Result(); err != nil {
			peer.Log.Errorf("set peer ttl failed: %v", err)
			return err
		}

		// Store the joint-set with task and set expiration.
		if _, err := pipe.SAdd(ctx, pkgredis.MakePersistentCachePeersOfPersistentCacheTaskInScheduler(p.config.Manager.SchedulerClusterID, peer.Task.ID), peer.ID).Result(); err != nil {
			peer.Log.Errorf("add peer id to task joint-set failed: %v", err)
			return err
		}

		if _, err := pipe.Expire(ctx, pkgredis.MakePersistentCachePeersOfPersistentCacheTaskInScheduler(p.config.Manager.SchedulerClusterID, peer.Host.ID), ttl).Result(); err != nil {
			peer.Log.Errorf("set task joint-set ttl failed: %v", err)
			return err
		}

		// Store the joint-set with task for persistent peer and set expiration.
		if peer.Persistent {
			if _, err := pipe.SAdd(ctx, pkgredis.MakePersistentPeersOfPersistentCacheTaskInScheduler(p.config.Manager.SchedulerClusterID, peer.Task.ID), peer.ID).Result(); err != nil {
				peer.Log.Errorf("add persistent peer id to task joint-set failed: %v", err)
				return err
			}

			if _, err := pipe.Expire(ctx, pkgredis.MakePersistentPeersOfPersistentCacheTaskInScheduler(p.config.Manager.SchedulerClusterID, peer.Host.ID), ttl).Result(); err != nil {
				peer.Log.Errorf("set task joint-set ttl failed: %v", err)
				return err
			}
		}

		// Store the joint-set with host.
		if _, err := pipe.SAdd(ctx, pkgredis.MakePersistentCachePeersOfPersistentCacheHostInScheduler(p.config.Manager.SchedulerClusterID, peer.Host.ID), peer.ID).Result(); err != nil {
			peer.Log.Errorf("add peer id to host joint-set failed: %v", err)
			return err
		}

		return nil
	}); err != nil {
		peer.Log.Errorf("store peer failed: %v", err)
		return err
	}

	return nil
}

// Delete deletes persistent cache peer by a key, and it will delete the association with task and host at the same time.
func (p *peerManager) Delete(ctx context.Context, peerID string) error {
	log := logger.WithPeerID(peerID)
	if _, err := p.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		rawPeer, err := p.rdb.HGetAll(ctx, pkgredis.MakePersistentCachePeerKeyInScheduler(p.config.Manager.SchedulerClusterID, peerID)).Result()
		if err != nil {
			return errors.New("getting peer failed from redis")
		}

		if _, err := pipe.Del(ctx, pkgredis.MakePersistentCachePeerKeyInScheduler(p.config.Manager.SchedulerClusterID, peerID)).Result(); err != nil {
			log.Errorf("delete peer failed: %v", err)
			return err
		}

		if _, err := pipe.SRem(ctx, pkgredis.MakePersistentCachePeersOfPersistentCacheTaskInScheduler(p.config.Manager.SchedulerClusterID, rawPeer["task_id"]), peerID).Result(); err != nil {
			log.Errorf("delete peer id from task joint-set failed: %v", err)
			return err
		}

		persistent, err := strconv.ParseBool(rawPeer["persistent"])
		if err != nil {
			log.Errorf("parsing persistent failed: %v", err)
			return err
		}

		if persistent {
			if _, err := pipe.SRem(ctx, pkgredis.MakePersistentPeersOfPersistentCacheTaskInScheduler(p.config.Manager.SchedulerClusterID, rawPeer["task_id"]), peerID).Result(); err != nil {
				log.Errorf("delete persistent peer id from task joint-set failed: %v", err)
			}
		}

		if _, err := pipe.SRem(ctx, pkgredis.MakePersistentCachePeersOfPersistentCacheHostInScheduler(p.config.Manager.SchedulerClusterID, rawPeer["host_id"]), peerID).Result(); err != nil {
			log.Errorf("delete peer id from host joint-set failed: %v", err)
			return err
		}

		return nil
	}); err != nil {
		log.Errorf("store peer failed: %v", err)
		return err
	}

	return nil
}

// LoadAll returns all persistent cache peers.
func (p *peerManager) LoadAll(ctx context.Context) ([]*Peer, error) {
	var (
		peers  []*Peer
		cursor uint64
	)

	for {
		var (
			peerKeys []string
			err      error
		)

		peerKeys, cursor, err = p.rdb.Scan(ctx, cursor, pkgredis.MakePersistentCachePeersInScheduler(p.config.Manager.SchedulerClusterID), 10).Result()
		if err != nil {
			logger.Error("scan tasks failed")
			return nil, err
		}

		for _, peerKey := range peerKeys {
			peer, loaded := p.Load(ctx, peerKey)
			if !loaded {
				logger.WithPeerID(peerKey).Error("load peer failed")
				continue
			}

			peers = append(peers, peer)
		}

		if cursor == 0 {
			break
		}
	}

	return peers, nil
}

// LoadAllByTaskID returns all persistent cache peers by task id.
func (p *peerManager) LoadAllByTaskID(ctx context.Context, taskID string) ([]*Peer, error) {
	log := logger.WithTaskID(taskID)
	peerIDs, err := p.rdb.SMembers(ctx, pkgredis.MakePersistentCachePeersOfPersistentCacheTaskInScheduler(p.config.Manager.SchedulerClusterID, taskID)).Result()
	if err != nil {
		log.Error("get peer ids failed")
		return nil, err
	}

	peers := make([]*Peer, 0, len(peerIDs))
	for _, peerID := range peerIDs {
		peer, loaded := p.Load(ctx, peerID)
		if !loaded {
			log.Errorf("load peer %s failed", peerID)
			continue
		}

		peers = append(peers, peer)
	}

	return peers, nil
}

// DeleteAllByTaskID deletes all persistent cache peers by task id.
func (p *peerManager) DeleteAllByTaskID(ctx context.Context, taskID string) error {
	log := logger.WithTaskID(taskID)
	peers, err := p.LoadAllByTaskID(ctx, taskID)
	if err != nil {
		log.Error("load peers failed")
		return err
	}

	for _, peer := range peers {
		addr := fmt.Sprintf("%s:%d", peer.Host.IP, peer.Host.Port)
		client, err := dfdaemonclient.GetV2ByAddr(ctx, addr, grpc.WithTransportCredentials(p.transportCredentials))
		if err != nil {
			log.Errorf("get dfdaemon client failed: %v", err)
			continue
		}

		if err := client.DeletePersistentCacheTask(ctx, &dfdaemonv2.DeletePersistentCacheTaskRequest{TaskId: taskID}); err != nil {
			log.Errorf("delete task %s failed", taskID)
			continue
		}

		if err := p.Delete(ctx, peer.ID); err != nil {
			log.Errorf("delete peer %s failed", peer.ID)
			continue
		}
	}

	return nil
}

// LoadAllByHostID returns all persistent cache peers by host id.
func (p *peerManager) LoadAllByHostID(ctx context.Context, hostID string) ([]*Peer, error) {
	log := logger.WithHostID(hostID)
	peerIDs, err := p.rdb.SMembers(ctx, pkgredis.MakePersistentCachePeersOfPersistentCacheHostInScheduler(p.config.Manager.SchedulerClusterID, hostID)).Result()
	if err != nil {
		log.Error("get peer ids failed")
		return nil, err
	}

	peers := make([]*Peer, 0, len(peerIDs))
	for _, peerID := range peerIDs {
		peer, loaded := p.Load(ctx, peerID)
		if !loaded {
			log.Errorf("load peer %s failed", peerID)
			continue
		}

		peers = append(peers, peer)
	}

	return peers, nil
}

// DeleteAllByHostID deletes all persistent cache peers by host id.
func (p *peerManager) DeleteAllByHostID(ctx context.Context, hostID string) error {
	log := logger.WithTaskID(hostID)
	peers, err := p.LoadAllByHostID(ctx, hostID)
	if err != nil {
		log.Error("load peers failed")
		return err
	}

	for _, peer := range peers {
		addr := fmt.Sprintf("%s:%d", peer.Host.IP, peer.Host.Port)
		client, err := dfdaemonclient.GetV2ByAddr(ctx, addr, grpc.WithTransportCredentials(p.transportCredentials))
		if err != nil {
			log.Errorf("get dfdaemon client failed: %v", err)
			continue
		}

		if err := client.DeletePersistentCacheTask(ctx, &dfdaemonv2.DeletePersistentCacheTaskRequest{TaskId: peer.Task.ID}); err != nil {
			log.Errorf("delete task %s failed", peer.Task.ID)
			continue
		}

		if err := p.Delete(ctx, peer.ID); err != nil {
			log.Errorf("delete peer %s failed", peer.ID)
			continue
		}
	}

	return nil
}
