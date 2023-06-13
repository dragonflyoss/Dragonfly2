/*
 *     Copyright 2022 The Dragonfly Authors
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

//go:generate mockgen -destination mocks/probe_mock.go -source probe.go -package mocks

package probe

import (
	"context"
	"io"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "d7y.io/api/pkg/apis/common/v1"
	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/client/config"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	pkgbalancer "d7y.io/dragonfly/v2/pkg/balancer"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/net/ping"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
)

type Probe interface {
	// Serve starts probe server.
	Serve() error

	// Stop stops probe server.
	Stop() error
}

type probe struct {
	config             *config.DaemonOption
	hostID             string
	daemonPort         int32
	daemonDownloadPort int32
	schedulerClient    schedulerclient.V1

	// syncProbesStream stands schedulerclient.Scheduler_SyncProbesClient from scheduler
	syncProbesStream schedulerv1.Scheduler_SyncProbesClient
	done             chan struct{}
}

// NewProbe returns a new Probe interface.
func NewProbe(cfg *config.DaemonOption, daemonPort int32, daemonDownloadPort int32, schedulerClient schedulerclient.V1) Probe {
	return &probe{
		config:             cfg,
		daemonPort:         daemonPort,
		daemonDownloadPort: daemonDownloadPort,
		schedulerClient:    schedulerClient,
		done:               make(chan struct{}),
	}
}

// Serve starts probe server.
func (p *probe) Serve() error {
	logger.Info("collect probes and upload probes to scheduler")
	if err := p.collectAndUploadProbesToScheduler(); err != nil {
		return err
	}

	return nil
}

// Stop stops probe server.
func (p *probe) Stop() error {
	close(p.done)
	return nil
}

// collectAndUploadProbesToScheduler collects probes and uploads probes to scheduler.
func (p *probe) collectAndUploadProbesToScheduler() error {
	ctx := context.WithValue(context.Background(), pkgbalancer.ContextKey, p.hostID)
	stream, err := p.schedulerClient.SyncProbes(ctx, &schedulerv1.SyncProbesRequest{
		Host: &v1.Host{
			Id:           idgen.HostIDV2(p.config.Host.AdvertiseIP.String(), p.config.Host.Hostname),
			Ip:           p.config.Host.AdvertiseIP.String(),
			Hostname:     p.config.Host.Hostname,
			Port:         p.daemonPort,
			DownloadPort: p.daemonDownloadPort,
			Location:     p.config.Host.Location,
			Idc:          p.config.Host.IDC,
		},
		Request: &schedulerv1.SyncProbesRequest_ProbeStartedRequest{
			ProbeStartedRequest: &schedulerv1.ProbeStartedRequest{},
		},
	})
	if err != nil {
		return err
	}

	syncProbesResponse, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return nil
		}

		logger.Errorf("receive error: %s", err.Error())
		return err
	}

	tick := time.NewTicker(syncProbesResponse.ProbeInterval.AsDuration())
	for {
		select {
		case <-tick.C:
			probes, failedProbes := p.collectProbes(syncProbesResponse.Hosts)
			if err := stream.Send(&schedulerv1.SyncProbesRequest{
				Host: &v1.Host{
					Id:           idgen.HostIDV2(p.config.Host.AdvertiseIP.String(), p.config.Host.Hostname),
					Ip:           p.config.Host.AdvertiseIP.String(),
					Hostname:     p.config.Host.Hostname,
					Port:         p.daemonPort,
					DownloadPort: p.daemonDownloadPort,
					Location:     p.config.Host.Location,
					Idc:          p.config.Host.IDC,
				},
				Request: &schedulerv1.SyncProbesRequest_ProbeFinishedRequest{
					ProbeFinishedRequest: &schedulerv1.ProbeFinishedRequest{
						Probes: probes,
					},
				},
			}); err != nil {
				return err
			}

			if err := stream.Send(&schedulerv1.SyncProbesRequest{
				Host: &v1.Host{
					Id:           idgen.HostIDV2(p.config.Host.AdvertiseIP.String(), p.config.Host.Hostname),
					Ip:           p.config.Host.AdvertiseIP.String(),
					Hostname:     p.config.Host.Hostname,
					Port:         p.daemonPort,
					DownloadPort: p.daemonDownloadPort,
					Location:     p.config.Host.Location,
					Idc:          p.config.Host.IDC,
				},
				Request: &schedulerv1.SyncProbesRequest_ProbeFailedRequest{
					ProbeFailedRequest: &schedulerv1.ProbeFailedRequest{
						Probes: failedProbes,
					},
				},
			}); err != nil {
				return err
			}

			syncProbesResponse, err = stream.Recv()
			if err != nil {
				if err == io.EOF {
					return nil
				}

				logger.Errorf("receive error: %s", err.Error())
				return err
			}
		case <-p.done:
			return nil
		}
	}
}

// collectProbes probes hosts and collect probes and failedProbes.
func (p *probe) collectProbes(desthosts []*v1.Host) ([]*schedulerv1.Probe, []*schedulerv1.FailedProbe) {
	probes := make([]*schedulerv1.Probe, 0)
	failedProbes := make([]*schedulerv1.FailedProbe, 0)
	for _, desthost := range desthosts {
		statistics, err := ping.Ping(desthost.Ip)
		if err != nil {
			failedProbes = append(failedProbes, &schedulerv1.FailedProbe{
				Host: &v1.Host{
					Id:           desthost.Id,
					Ip:           desthost.Ip,
					Hostname:     desthost.Hostname,
					Port:         desthost.Port,
					DownloadPort: desthost.DownloadPort,
					Location:     desthost.Location,
					Idc:          desthost.Idc,
				},
				Description: err.Error(),
			})

			continue
		}

		probes = append(probes, &schedulerv1.Probe{
			Host: &v1.Host{
				Id:           desthost.Id,
				Ip:           desthost.Ip,
				Hostname:     desthost.Hostname,
				Port:         desthost.Port,
				DownloadPort: desthost.DownloadPort,
				Location:     desthost.Location,
				Idc:          desthost.Idc,
			},
			Rtt:       durationpb.New(statistics.AvgRtt),
			CreatedAt: timestamppb.New(time.Now()),
		})
	}

	return probes, failedProbes
}
