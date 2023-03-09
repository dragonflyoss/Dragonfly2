package probe

import (
	"context"
	v1 "d7y.io/api/pkg/apis/common/v1"
	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"
	"d7y.io/dragonfly/v2/client/config"
	pkgbalancer "d7y.io/dragonfly/v2/pkg/balancer"
	"d7y.io/dragonfly/v2/pkg/net/ping"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

const (
	DefaultProbeSyncInterval = 30 * time.Second
)

// Probe is the interface used for probeManager service.
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

// Option is a functional option for configuring the announcer.
type Option func(p *probe)

// New returns a new Probe interface.
func New(cfg *config.DaemonOption, hostID string, daemonPort int32, daemonDownloadPort int32, schedulerClient schedulerclient.V1) Probe {
	p := &probe{
		config:             cfg,
		hostID:             hostID,
		daemonPort:         daemonPort,
		daemonDownloadPort: daemonDownloadPort,
		schedulerClient:    schedulerClient,
		done:               make(chan struct{}),
	}
	return p
}

func (p *probe) Serve() error {

	rawHost := &v1.Host{
		Id:             p.hostID,
		Ip:             p.config.Host.AdvertiseIP.String(),
		Hostname:       p.config.Host.Hostname,
		Port:           p.daemonPort,
		DownloadPort:   p.daemonDownloadPort,
		SecurityDomain: p.config.Host.SecurityDomain,
		Location:       p.config.Host.Location,
		Idc:            p.config.Host.IDC,
	}
	probesOfHostWithoutProbes := &schedulerv1.ProbesOfHost{
		Host:   rawHost,
		Probes: make([]*schedulerv1.Probe, 0),
	}
	syncProbesRequestWithoutProbes := &schedulerv1.SyncProbesRequest{ProbesOfHost: probesOfHostWithoutProbes}
	schedulerSyncProbesClient, err := p.schedulerClient.SyncProbes(context.WithValue(context.Background(), pkgbalancer.ContextKey, p.hostID),
		syncProbesRequestWithoutProbes)
	if err != nil {
		return err
	}
	p.syncProbesStream = schedulerSyncProbesClient

	tick := time.NewTicker(DefaultProbeSyncInterval)
	for {
		select {
		case <-tick.C:
			syncProbesResponse, err := p.syncProbesStream.Recv()
			if err != nil {
				return err
			}
			probes := make([]*schedulerv1.Probe, 0)
			for _, host := range syncProbesResponse.Hosts {
				statistics, err := ping.Ping(host.Ip)
				if err != nil {
					continue
				} else {
					rawProbe := &schedulerv1.Probe{
						Host: &v1.Host{
							Id:             host.Id,
							Ip:             host.Ip,
							Hostname:       host.Hostname,
							Port:           host.Port,
							DownloadPort:   host.DownloadPort,
							SecurityDomain: host.SecurityDomain,
							Location:       host.Location,
							Idc:            host.Idc,
						},
						Rtt:       durationpb.New(statistics.AvgRtt),
						UpdatedAt: timestamppb.New(time.Now()),
					}
					probes = append(probes, rawProbe)
				}
			}
			probesOfHost := &schedulerv1.ProbesOfHost{
				Host:   rawHost,
				Probes: probes,
			}
			syncProbesRequest := &schedulerv1.SyncProbesRequest{ProbesOfHost: probesOfHost}
			schedulerSyncProbesClient, err := p.schedulerClient.SyncProbes(context.WithValue(context.Background(), pkgbalancer.ContextKey, p.hostID),
				syncProbesRequest)
			if err != nil {
				return err
			}
			p.syncProbesStream = schedulerSyncProbesClient
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
