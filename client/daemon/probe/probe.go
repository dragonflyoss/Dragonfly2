package probe

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "d7y.io/api/pkg/apis/common/v1"
	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"
	pkgbalancer "d7y.io/dragonfly/v2/pkg/balancer"
	"d7y.io/dragonfly/v2/pkg/net/ping"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"

	"d7y.io/dragonfly/v2/client/config"
)

const (
	DefaultProbeInterval = 20 * time.Minute
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
func NewProbe(cfg *config.DaemonOption, hostID string, daemonPort int32, daemonDownloadPort int32, schedulerClient schedulerclient.V1) Probe {
	return &probe{
		config:             cfg,
		hostID:             hostID,
		daemonPort:         daemonPort,
		daemonDownloadPort: daemonDownloadPort,
		schedulerClient:    schedulerClient,
		done:               make(chan struct{}),
	}
}

func (p *probe) Serve() error {
	daemonHost := &v1.Host{
		Id:           p.hostID,
		Ip:           p.config.Host.AdvertiseIP.String(),
		Hostname:     p.config.Host.Hostname,
		Port:         p.daemonPort,
		DownloadPort: p.daemonDownloadPort,
		Location:     p.config.Host.Location,
		Idc:          p.config.Host.IDC,
	}

	probesOfHost := &schedulerv1.ProbesOfHost{
		Host:   daemonHost,
		Probes: make([]*schedulerv1.Probe, 0),
	}

	syncProbesRequest := &schedulerv1.SyncProbesRequest{ProbesOfHost: probesOfHost}
	syncProbesClient, err := p.schedulerClient.SyncProbes(context.WithValue(context.Background(), pkgbalancer.ContextKey, p.hostID), syncProbesRequest)
	if err != nil {
		return err
	}

	p.syncProbesStream = syncProbesClient
	syncProbesResponse, err := p.syncProbesStream.Recv()
	if err != nil {
		return err
	}

	// The interval is modified according to the probe configuration of the scheduler.
	tick := time.NewTicker(syncProbesResponse.ProbeInterval.AsDuration())
	for {
		select {
		case <-tick.C:
			probes := make([]*schedulerv1.Probe, 0)
			for _, host := range syncProbesResponse.Hosts {
				statistics, err := ping.Ping(host.Ip)
				if err != nil {
					continue
				}

				probe := &schedulerv1.Probe{
					Host: &v1.Host{
						Id:           host.Id,
						Ip:           host.Ip,
						Hostname:     host.Hostname,
						Port:         host.Port,
						DownloadPort: host.DownloadPort,
						Location:     host.Location,
						Idc:          host.Idc,
					},
					Rtt:       durationpb.New(statistics.AvgRtt),
					CreatedAt: timestamppb.New(time.Now()),
				}

				probes = append(probes, probe)
			}

			probesOfHost := &schedulerv1.ProbesOfHost{
				Host:   daemonHost,
				Probes: probes,
			}

			syncProbesRequest := &schedulerv1.SyncProbesRequest{ProbesOfHost: probesOfHost}
			schedulerSyncProbesClient, err := p.schedulerClient.SyncProbes(context.WithValue(context.Background(), pkgbalancer.ContextKey, p.hostID),
				syncProbesRequest)
			if err != nil {
				return err
			}

			p.syncProbesStream = schedulerSyncProbesClient
			syncProbesResponse, err = p.syncProbesStream.Recv()
			if err != nil {
				return err
			}

		case <-p.done:
			return nil
		}

		return nil
	}
}

func (p *probe) Stop() error {
	close(p.done)
	return nil
}
