/*
 *     Copyright 2023 The Dragonfly Authors
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

//go:generate mockgen -destination mocks/network_topology_mock.go -source network_topology.go -package mocks

package networktopology

import (
	"context"
	"io"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "d7y.io/api/v2/pkg/apis/common/v1"
	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/client/config"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/net/ping"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
)

type NetworkTopology interface {
	// Serve starts network topology server.
	Serve()

	// Stop stops network topology server.
	Stop()
}

// networkTopology implements NetworkTopology.
type networkTopology struct {
	config             *config.DaemonOption
	hostID             string
	daemonPort         int32
	daemonDownloadPort int32
	schedulerClient    schedulerclient.V1
	done               chan struct{}
}

// NewNetworkTopology returns a new NetworkTopology interface.
func NewNetworkTopology(cfg *config.DaemonOption, hostID string, daemonPort int32, daemonDownloadPort int32,
	schedulerClient schedulerclient.V1) (NetworkTopology, error) {
	return &networkTopology{
		config:             cfg,
		hostID:             hostID,
		daemonPort:         daemonPort,
		daemonDownloadPort: daemonDownloadPort,
		schedulerClient:    schedulerClient,
		done:               make(chan struct{}),
	}, nil
}

// Serve starts network topology server.
func (nt *networkTopology) Serve() {
	tick := time.NewTicker(nt.config.NetworkTopology.Probe.Interval)
	for {
		select {
		case <-tick.C:
			if err := nt.syncProbes(); err != nil {
				logger.Error(err)
			}
		case <-nt.done:
			return
		}
	}
}

// Stop stops network topology server.
func (nt *networkTopology) Stop() {
	close(nt.done)
}

// syncProbes syncs probes to scheduler.
func (nt *networkTopology) syncProbes() error {
	host := &v1.Host{
		Id:           nt.hostID,
		Ip:           nt.config.Host.AdvertiseIP.String(),
		Hostname:     nt.config.Host.Hostname,
		Port:         nt.daemonPort,
		DownloadPort: nt.daemonDownloadPort,
		Location:     nt.config.Host.Location,
		Idc:          nt.config.Host.IDC,
	}

	stream, err := nt.schedulerClient.SyncProbes(context.Background(), &schedulerv1.SyncProbesRequest{
		Host: host,
		Request: &schedulerv1.SyncProbesRequest_ProbeStartedRequest{
			ProbeStartedRequest: &schedulerv1.ProbeStartedRequest{},
		},
	})
	if err != nil {
		return err
	}

	resp, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return nil
		}

		return err
	}

	// Ping the destination host with the ICMP protocol.
	probes, failedProbes := nt.pingHosts(resp.Hosts)
	if len(probes) > 0 {
		if err := stream.Send(&schedulerv1.SyncProbesRequest{
			Host: host,
			Request: &schedulerv1.SyncProbesRequest_ProbeFinishedRequest{
				ProbeFinishedRequest: &schedulerv1.ProbeFinishedRequest{
					Probes: probes,
				},
			},
		}); err != nil {
			return err
		}
	}

	if len(failedProbes) > 0 {
		if err := stream.Send(&schedulerv1.SyncProbesRequest{
			Host: host,
			Request: &schedulerv1.SyncProbesRequest_ProbeFailedRequest{
				ProbeFailedRequest: &schedulerv1.ProbeFailedRequest{
					Probes: failedProbes,
				},
			},
		}); err != nil {
			return err
		}
	}

	return nil
}

// Ping the destination host with the ICMP protocol. If the host is unreachable,
// we will send the failed probe result to the scheduler. If the host is reachable,
// we will send the probe result to the scheduler.
func (nt *networkTopology) pingHosts(destHosts []*v1.Host) ([]*schedulerv1.Probe, []*schedulerv1.FailedProbe) {
	var (
		probes       []*schedulerv1.Probe
		failedProbes []*schedulerv1.FailedProbe
	)

	wg := &sync.WaitGroup{}
	wg.Add(len(destHosts))
	for _, destHost := range destHosts {
		go func(destHost *v1.Host) {
			defer wg.Done()

			stats, err := ping.Ping(destHost.Ip)
			if err != nil {
				failedProbes = append(failedProbes, &schedulerv1.FailedProbe{
					Host: &v1.Host{
						Id:           destHost.Id,
						Ip:           destHost.Ip,
						Hostname:     destHost.Hostname,
						Port:         destHost.Port,
						DownloadPort: destHost.DownloadPort,
						Location:     destHost.Location,
						Idc:          destHost.Idc,
					},
					Description: err.Error(),
				})

				return
			}

			probes = append(probes, &schedulerv1.Probe{
				Host: &v1.Host{
					Id:           destHost.Id,
					Ip:           destHost.Ip,
					Hostname:     destHost.Hostname,
					Port:         destHost.Port,
					DownloadPort: destHost.DownloadPort,
					Location:     destHost.Location,
					Idc:          destHost.Idc,
				},
				Rtt:       durationpb.New(stats.AvgRtt),
				CreatedAt: timestamppb.New(time.Now()),
			})
		}(destHost)
	}

	wg.Wait()
	return probes, failedProbes
}
