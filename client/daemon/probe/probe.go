package probe

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "d7y.io/api/pkg/apis/common/v1"
	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/client/config"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	pkgbalancer "d7y.io/dragonfly/v2/pkg/balancer"
	"d7y.io/dragonfly/v2/pkg/net/ping"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
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

// Serve starts probe server.
func (p *probe) Serve() error {
	logger.Info("upload probes to scheduler")
	if err := p.syncProbes(); err != nil {
		return err
	}

	return nil
}

// Stop stops probe server.
func (p *probe) Stop() error {
	close(p.done)
	return nil
}

// syncProbes uploads probes to scheduler for synchronizing probe results.
func (p *probe) syncProbes() error {
	if err := p.uploadProbesToScheduler([]*schedulerv1.Probe{}); err != nil {
		logger.Error(err)
	}

	syncProbesResponse, err := p.syncProbesStream.Recv()
	if err != nil {
		return err
	}

	tick := time.NewTicker(syncProbesResponse.ProbeInterval.AsDuration())
	for {
		select {
		case <-tick.C:
			probes := p.collectProbes(syncProbesResponse.Hosts)
			if err := p.uploadProbesToScheduler(probes); err != nil {
				logger.Error(err)
			}

			syncProbesResponse, err = p.syncProbesStream.Recv()
			if err != nil {
				return err
			}

		case <-p.done:
			return nil
		}
	}
}

// collectProbes probes hosts and collect result.
func (p *probe) collectProbes(hosts []*v1.Host) []*schedulerv1.Probe {
	probes := make([]*schedulerv1.Probe, 0)
	for _, host := range hosts {
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

	return probes
}

// uploadProbesToScheduler uploads probes to scheduler.
func (p *probe) uploadProbesToScheduler(probes []*schedulerv1.Probe) error {
	ctx := context.WithValue(context.Background(), pkgbalancer.ContextKey, p.hostID)
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
		Probes: probes,
	}

	syncProbesRequest := &schedulerv1.SyncProbesRequest{ProbesOfHost: probesOfHost}
	schedulerSyncProbesClient, err := p.schedulerClient.SyncProbes(ctx, syncProbesRequest)
	if err != nil {
		return err
	}

	p.syncProbesStream = schedulerSyncProbesClient
	return nil
}
