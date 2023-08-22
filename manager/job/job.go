/*
 *     Copyright 2020 The Dragonfly Authors
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

package job

import (
	"crypto/x509"
	"errors"

	"go.opentelemetry.io/otel"

	internaljob "d7y.io/dragonfly/v2/internal/job"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/models"
)

// tracer is a global tracer for job.
var tracer = otel.Tracer("manager")

// Job is an implementation of job.
type Job struct {
	*internaljob.Job
	Preheat
	SyncPeers
}

// New returns a new Job.
func New(cfg *config.Config) (*Job, error) {
	j, err := internaljob.New(&internaljob.Config{
		Addrs:      cfg.Database.Redis.Addrs,
		MasterName: cfg.Database.Redis.MasterName,
		Username:   cfg.Database.Redis.Username,
		Password:   cfg.Database.Redis.Password,
		BrokerDB:   cfg.Database.Redis.BrokerDB,
		BackendDB:  cfg.Database.Redis.BackendDB,
	}, internaljob.GlobalQueue)
	if err != nil {
		return nil, err
	}

	var certPool *x509.CertPool
	if cfg.Job.Preheat.TLS != nil {
		certPool = x509.NewCertPool()
		if !certPool.AppendCertsFromPEM([]byte(cfg.Job.Preheat.TLS.CACert)) {
			return nil, errors.New("invalid CA Cert")
		}
	}

	preheat, err := newPreheat(j, cfg.Job.Preheat.RegistryTimeout, certPool)
	if err != nil {
		return nil, err
	}

	syncPeers, err := newSyncPeers(j)
	if err != nil {
		return nil, err
	}

	return &Job{
		Job:       j,
		Preheat:   preheat,
		SyncPeers: syncPeers,
	}, nil
}

// getSchedulerQueues gets scheduler queues.
func getSchedulerQueues(schedulers []models.Scheduler) []internaljob.Queue {
	var queues []internaljob.Queue
	for _, scheduler := range schedulers {
		queue, err := internaljob.GetSchedulerQueue(scheduler.SchedulerClusterID, scheduler.Hostname)
		if err != nil {
			continue
		}

		queues = append(queues, queue)
	}

	return queues
}
