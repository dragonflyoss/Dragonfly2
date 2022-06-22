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

package daemon

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/gc"
	"d7y.io/dragonfly/v2/client/daemon/metrics"
	"d7y.io/dragonfly/v2/client/daemon/objectstorage"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	"d7y.io/dragonfly/v2/client/daemon/proxy"
	"d7y.io/dragonfly/v2/client/daemon/rpcserver"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/client/daemon/upload"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/dfpath"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/reachable"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
	"d7y.io/dragonfly/v2/pkg/source"
)

type Daemon interface {
	Serve() error
	Stop()

	// ExportTaskManager returns the underlay peer.TaskManager for downloading when embed dragonfly in custom binary
	ExportTaskManager() peer.TaskManager
	// ExportPeerHost returns the underlay scheduler.PeerHost for scheduling
	ExportPeerHost() *scheduler.PeerHost
}

type clientDaemon struct {
	once *sync.Once
	done chan bool

	schedPeerHost *scheduler.PeerHost

	Option config.DaemonOption

	RPCManager     rpcserver.Server
	UploadManager  upload.Manager
	ObjectStorage  objectstorage.ObjectStorage
	ProxyManager   proxy.Manager
	StorageManager storage.Manager
	GCManager      gc.Manager

	PeerTaskManager peer.TaskManager
	PieceManager    peer.PieceManager

	dynconfig       config.Dynconfig
	dfpath          dfpath.Dfpath
	schedulers      []*manager.Scheduler
	managerClient   managerclient.Client
	schedulerClient schedulerclient.Client
}

func New(opt *config.DaemonOption, d dfpath.Dfpath) (Daemon, error) {
	// update plugin directory
	source.UpdatePluginDir(d.PluginDir())

	host := &scheduler.PeerHost{
		Id:             idgen.HostID(opt.Host.Hostname, int32(opt.Download.PeerGRPC.TCPListen.PortRange.Start)),
		Ip:             opt.Host.AdvertiseIP,
		RpcPort:        int32(opt.Download.PeerGRPC.TCPListen.PortRange.Start),
		DownPort:       0,
		HostName:       opt.Host.Hostname,
		SecurityDomain: opt.Host.SecurityDomain,
		Location:       opt.Host.Location,
		Idc:            opt.Host.IDC,
		NetTopology:    opt.Host.NetTopology,
	}

	var (
		addrs          []dfnet.NetAddr
		schedulers     []*manager.Scheduler
		dynconfig      config.Dynconfig
		managerClient  managerclient.Client
		defaultPattern = config.ConvertPattern(opt.Download.DefaultPattern, base.Pattern_P2P)
	)

	if opt.Scheduler.Manager.Enable == true {
		// New manager client
		var err error
		managerClient, err = managerclient.NewWithAddrs(opt.Scheduler.Manager.NetAddrs)
		if err != nil {
			return nil, err
		}

		// New dynconfig client
		dynconfig, err = config.NewDynconfig(managerClient, d.CacheDir(), opt.Host, opt.Scheduler.Manager.RefreshInterval)
		if err != nil {
			return nil, err
		}

		// Get schedulers from manager
		schedulers, err = dynconfig.GetSchedulers()
		if err != nil {
			return nil, err
		}

		addrs = schedulersToAvailableNetAddrs(schedulers)
	} else {
		addrs = opt.Scheduler.NetAddrs
	}
	logger.Infof("initialize scheduler addresses: %#v", addrs)

	var opts []grpc.DialOption
	if opt.Options.Telemetry.Jaeger != "" {
		opts = append(opts,
			grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
			grpc.WithChainStreamInterceptor(otelgrpc.StreamClientInterceptor()),
		)
	}
	sched, err := schedulerclient.GetClientByAddr(addrs, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get schedulers")
	}

	// Storage.Option.DataPath is same with Daemon DataDir
	opt.Storage.DataPath = d.DataDir()
	gcCallback := func(request storage.CommonTaskRequest) {
		er := sched.LeaveTask(context.Background(), &scheduler.PeerTarget{
			TaskId: request.TaskID,
			PeerId: request.PeerID,
		})
		if er != nil {
			logger.Errorf("step 4:leave task %s/%s, error: %v", request.TaskID, request.PeerID, er)
		} else {
			logger.Infof("step 4:leave task %s/%s state ok", request.TaskID, request.PeerID)
		}
	}
	storageManager, err := storage.NewStorageManager(opt.Storage.StoreStrategy, &opt.Storage,
		gcCallback, storage.WithGCInterval(opt.GCInterval.Duration))
	if err != nil {
		return nil, err
	}

	pieceManager, err := peer.NewPieceManager(
		opt.Download.PieceDownloadTimeout,
		peer.WithLimiter(rate.NewLimiter(opt.Download.TotalRateLimit.Limit, int(opt.Download.TotalRateLimit.Limit))),
		peer.WithCalculateDigest(opt.Download.CalculateDigest), peer.WithTransportOption(opt.Download.TransportOption),
	)
	if err != nil {
		return nil, err
	}
	peerTaskManager, err := peer.NewPeerTaskManager(host, pieceManager, storageManager, sched, opt.Scheduler,
		opt.Download.PerPeerRateLimit.Limit, opt.Storage.Multiplex, opt.Download.Prefetch, opt.Download.CalculateDigest,
		opt.Download.GetPiecesMaxRetry, opt.Download.WatchdogTimeout)
	if err != nil {
		return nil, err
	}

	// TODO(jim): more server options
	var downloadServerOption []grpc.ServerOption
	if !opt.Download.DownloadGRPC.Security.Insecure {
		tlsCredentials, err := loadGPRCTLSCredentials(opt.Download.DownloadGRPC.Security)
		if err != nil {
			return nil, err
		}
		downloadServerOption = append(downloadServerOption, grpc.Creds(tlsCredentials))
	}
	var peerServerOption []grpc.ServerOption
	if !opt.Download.PeerGRPC.Security.Insecure {
		tlsCredentials, err := loadGPRCTLSCredentials(opt.Download.PeerGRPC.Security)
		if err != nil {
			return nil, err
		}
		peerServerOption = append(peerServerOption, grpc.Creds(tlsCredentials))
	}
	rpcManager, err := rpcserver.New(host, peerTaskManager, storageManager, defaultPattern, downloadServerOption, peerServerOption)
	if err != nil {
		return nil, err
	}

	var proxyManager proxy.Manager
	proxyManager, err = proxy.NewProxyManager(host, peerTaskManager, defaultPattern, opt.Proxy)
	if err != nil {
		return nil, err
	}

	uploadManager, err := upload.NewUploadManager(opt, storageManager, d.LogDir(),
		upload.WithLimiter(rate.NewLimiter(opt.Upload.RateLimit.Limit, int(opt.Upload.RateLimit.Limit))))
	if err != nil {
		return nil, err
	}

	objectStorage, err := objectstorage.New(opt, dynconfig, peerTaskManager, storageManager, d.LogDir())
	if err != nil {
		return nil, err
	}

	return &clientDaemon{
		once:            &sync.Once{},
		done:            make(chan bool),
		schedPeerHost:   host,
		Option:          *opt,
		RPCManager:      rpcManager,
		PeerTaskManager: peerTaskManager,
		PieceManager:    pieceManager,
		ProxyManager:    proxyManager,
		UploadManager:   uploadManager,
		ObjectStorage:   objectStorage,
		StorageManager:  storageManager,
		GCManager:       gc.NewManager(opt.GCInterval.Duration),
		dynconfig:       dynconfig,
		dfpath:          d,
		schedulers:      schedulers,
		managerClient:   managerClient,
		schedulerClient: sched,
	}, nil
}

func loadGPRCTLSCredentials(opt config.SecurityOption) (credentials.TransportCredentials, error) {
	// Load certificate of the CA who signed client's certificate
	pemClientCA, err := os.ReadFile(opt.CACert)
	if err != nil {
		return nil, err
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(pemClientCA) {
		return nil, fmt.Errorf("failed to add client CA's certificate")
	}

	// Load server's certificate and private key
	serverCert, err := tls.LoadX509KeyPair(opt.Cert, opt.Key)
	if err != nil {
		return nil, err
	}

	// Create the credentials and return it
	if opt.TLSConfig == nil {
		opt.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{serverCert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    certPool,
		}
	} else {
		opt.TLSConfig.Certificates = []tls.Certificate{serverCert}
		opt.TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
		opt.TLSConfig.ClientCAs = certPool
	}

	return credentials.NewTLS(opt.TLSConfig), nil
}

func (*clientDaemon) prepareTCPListener(opt config.ListenOption, withTLS bool) (net.Listener, int, error) {
	if len(opt.TCPListen.Namespace) > 0 {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		recoverFunc, err := switchNetNamespace(opt.TCPListen.Namespace)
		if err != nil {
			logger.Errorf("failed to change net namespace: %v", err)
			return nil, -1, err
		}
		defer func() {
			err := recoverFunc()
			if err != nil {
				logger.Errorf("failed to recover net namespace: %v", err)
			}
		}()
	}

	var (
		ln   net.Listener
		port int
		err  error
	)
	if opt.TCPListen == nil {
		return nil, -1, errors.New("empty tcp listen option")
	}

	ln, port, err = rpc.ListenWithPortRange(opt.TCPListen.Listen, opt.TCPListen.PortRange.Start, opt.TCPListen.PortRange.End)
	if err != nil {
		return nil, -1, err
	}

	// when use grpc, tls config is in server option
	if !withTLS || opt.Security.Insecure {
		return ln, port, err
	}

	if opt.Security.Cert == "" || opt.Security.Key == "" {
		return nil, -1, errors.New("empty cert or key for tls")
	}

	// Create the TLS ClientOption with the CA pool and enable Client certificate validation
	if opt.Security.TLSConfig == nil {
		opt.Security.TLSConfig = &tls.Config{}
	}

	tlsConfig := opt.Security.TLSConfig
	if opt.Security.CACert != "" {
		caCert, err := os.ReadFile(opt.Security.CACert)
		if err != nil {
			return nil, -1, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.ClientCAs = caCertPool
		if opt.Security.TLSVerify {
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		}
	}

	tlsConfig.Certificates = make([]tls.Certificate, 1)
	tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(opt.Security.Cert, opt.Security.Key)
	if err != nil {
		return nil, -1, err
	}

	return tls.NewListener(ln, tlsConfig), port, nil
}

func (cd *clientDaemon) Serve() error {
	cd.GCManager.Start()
	// prepare download service listen
	if cd.Option.Download.DownloadGRPC.UnixListen == nil {
		return errors.New("download grpc unix listen option is empty")
	}
	cd.Option.Download.DownloadGRPC.UnixListen.Socket = cd.dfpath.DaemonSockPath()
	if cd.Option.Download.DownloadGRPC.UnixListen.Socket == "" {
		return errors.New("download grpc unix listen socket is empty")
	}
	_ = os.Remove(cd.Option.Download.DownloadGRPC.UnixListen.Socket)
	downloadListener, err := rpc.Listen(dfnet.NetAddr{
		Type: dfnet.UNIX,
		Addr: cd.Option.Download.DownloadGRPC.UnixListen.Socket,
	})
	if err != nil {
		logger.Errorf("failed to listen for download grpc service: %v", err)
		return err
	}

	// prepare peer service listen
	if cd.Option.Download.PeerGRPC.TCPListen == nil {
		return errors.New("peer grpc tcp listen option is empty")
	}
	peerListener, peerPort, err := cd.prepareTCPListener(cd.Option.Download.PeerGRPC, false)
	if err != nil {
		logger.Errorf("failed to listen for peer grpc service: %v", err)
		return err
	}
	cd.schedPeerHost.RpcPort = int32(peerPort)

	// prepare upload service listen
	if cd.Option.Upload.TCPListen == nil {
		return errors.New("upload tcp listen option is empty")
	}
	uploadListener, uploadPort, err := cd.prepareTCPListener(cd.Option.Upload.ListenOption, true)
	if err != nil {
		logger.Errorf("failed to listen for upload service: %v", err)
		return err
	}
	cd.schedPeerHost.DownPort = int32(uploadPort)

	// prepare object storage service listen
	if cd.Option.ObjectStorage.TCPListen == nil {
		return errors.New("object storage tcp listen option is empty")
	}
	objectStorageListener, _, err := cd.prepareTCPListener(cd.Option.ObjectStorage.ListenOption, true)
	if err != nil {
		logger.Errorf("failed to listen for object storage service: %v", err)
		return err
	}

	g := errgroup.Group{}
	// serve download grpc service
	g.Go(func() error {
		defer downloadListener.Close()
		logger.Infof("serve download grpc at unix://%s", cd.Option.Download.DownloadGRPC.UnixListen.Socket)
		if err := cd.RPCManager.ServeDownload(downloadListener); err != nil {
			logger.Errorf("failed to serve for download grpc service: %v", err)
			return err
		}
		return nil
	})

	// serve peer grpc service
	g.Go(func() error {
		defer peerListener.Close()
		logger.Infof("serve peer grpc at %s://%s", peerListener.Addr().Network(), peerListener.Addr().String())
		if err := cd.RPCManager.ServePeer(peerListener); err != nil {
			logger.Errorf("failed to serve for peer grpc service: %v", err)
			return err
		}
		return nil
	})

	if cd.ProxyManager.IsEnabled() {
		// prepare proxy service listen
		if cd.Option.Proxy.TCPListen == nil {
			return errors.New("proxy tcp listen option is empty")
		}
		proxyListener, proxyPort, err := cd.prepareTCPListener(cd.Option.Proxy.ListenOption, true)
		if err != nil {
			logger.Errorf("failed to listen for proxy service: %v", err)
			return err
		}
		// serve proxy service
		g.Go(func() error {
			defer proxyListener.Close()
			logger.Infof("serve proxy at tcp://%s:%d", cd.Option.Proxy.TCPListen.Listen, proxyPort)
			if err = cd.ProxyManager.Serve(proxyListener); err != nil && err != http.ErrServerClosed {
				logger.Errorf("failed to serve for proxy service: %v", err)
				return err
			} else if err == http.ErrServerClosed {
				logger.Infof("proxy service closed")
			}
			return nil
		})
		// serve proxy sni service
		if cd.Option.Proxy.HijackHTTPS != nil && len(cd.Option.Proxy.HijackHTTPS.SNI) > 0 {
			for _, opt := range cd.Option.Proxy.HijackHTTPS.SNI {
				listener, port, err := cd.prepareTCPListener(config.ListenOption{
					TCPListen: opt,
				}, false)
				if err != nil {
					logger.Errorf("failed to listen for proxy sni service: %v", err)
					return err
				}
				logger.Infof("serve proxy sni at tcp://%s:%d", opt.Listen, port)

				g.Go(func() error {
					err := cd.ProxyManager.ServeSNI(listener)
					if err != nil {
						logger.Errorf("failed to serve proxy sni service: %v", err)
					}
					return err
				})
			}
		}
	}

	// serve upload service
	g.Go(func() error {
		defer uploadListener.Close()
		logger.Infof("serve upload service at %s://%s", uploadListener.Addr().Network(), uploadListener.Addr().String())
		if err := cd.UploadManager.Serve(uploadListener); err != nil && err != http.ErrServerClosed {
			logger.Errorf("failed to serve for upload service: %v", err)
			return err
		} else if err == http.ErrServerClosed {
			logger.Infof("upload service closed")
		}
		return nil
	})

	// serve object storage service
	g.Go(func() error {
		defer objectStorageListener.Close()
		logger.Infof("serve object storage service at %s://%s", objectStorageListener.Addr().Network(), objectStorageListener.Addr().String())
		if err := cd.ObjectStorage.Serve(objectStorageListener); err != nil && err != http.ErrServerClosed {
			logger.Errorf("failed to serve for object storage service: %v", err)
			return err
		} else if err == http.ErrServerClosed {
			logger.Infof("object storage service closed")
		}
		return nil
	})

	// enable seed peer mode
	if cd.managerClient != nil && cd.Option.Scheduler.Manager.SeedPeer.Enable {
		logger.Info("announce to manager")
		if err := cd.announceSeedPeer(); err != nil {
			return err
		}

		g.Go(func() error {
			logger.Info("keepalive to manager")
			cd.managerClient.KeepAlive(cd.Option.Scheduler.Manager.SeedPeer.KeepAlive.Interval, &manager.KeepAliveRequest{
				SourceType: manager.SourceType_SEED_PEER_SOURCE,
				HostName:   cd.Option.Host.Hostname,
				ClusterId:  uint64(cd.Option.Scheduler.Manager.SeedPeer.ClusterID),
			})
			return err
		})
	}

	if cd.Option.AliveTime.Duration > 0 {
		g.Go(func() error {
			for {
				select {
				case <-time.After(cd.Option.AliveTime.Duration):
					var keepalives = []clientutil.KeepAlive{
						cd.StorageManager,
						cd.RPCManager,
					}
					var keep bool
					for _, keepalive := range keepalives {
						if keepalive.Alive(cd.Option.AliveTime.Duration) {
							keep = true
							break
						}
					}
					if !keep {
						cd.Stop()
						logger.Infof("alive time reached, stop daemon")
						return nil
					}
				case <-cd.done:
					logger.Infof("peer host done, stop watch alive time")
					return nil
				}
			}
		})
	}

	// serve dynconfig service
	if cd.dynconfig != nil {
		// dynconfig register client daemon
		cd.dynconfig.Register(cd)

		// serve dynconfig
		g.Go(func() error {
			if err := cd.dynconfig.Serve(); err != nil {
				logger.Errorf("dynconfig start failed %v", err)
				return err
			}
			logger.Info("dynconfig start successfully")
			return nil
		})
	}

	if cd.Option.Metrics != "" {
		metricsServer := metrics.New(cd.Option.Metrics)
		go func() {
			logger.Infof("started metrics server at %s", metricsServer.Addr)
			if err := metricsServer.ListenAndServe(); err != nil {
				if err == http.ErrServerClosed {
					return
				}
				logger.Fatalf("metrics server closed unexpect: %v", err)
			}
		}()
	}

	if cd.Option.Health != nil {
		if cd.Option.Health.ListenOption.TCPListen == nil {
			logger.Fatalf("health listen not found")
		}

		r := gin.Default()
		r.GET(cd.Option.Health.Path, func(c *gin.Context) {
			c.JSON(http.StatusOK, http.StatusText(http.StatusOK))
		})

		listener, _, err := cd.prepareTCPListener(cd.Option.Health.ListenOption, false)
		if err != nil {
			logger.Fatalf("init health http server error: %v", err)
		}

		go func() {
			logger.Infof("serve http health at %#v", cd.Option.Health.ListenOption.TCPListen)
			if err = http.Serve(listener, r); err != nil {
				if err == http.ErrServerClosed {
					return
				}
				logger.Errorf("health http server error: %v", err)
			}
		}()
	}

	werr := g.Wait()
	cd.Stop()
	return werr
}

func (cd *clientDaemon) Stop() {
	cd.once.Do(func() {
		close(cd.done)
		cd.GCManager.Stop()
		cd.RPCManager.Stop()
		if err := cd.UploadManager.Stop(); err != nil {
			logger.Errorf("upload manager stop failed %s", err)
		}

		if err := cd.ObjectStorage.Stop(); err != nil {
			logger.Errorf("object storage stop failed %s", err)
		}

		if cd.ProxyManager.IsEnabled() {
			if err := cd.ProxyManager.Stop(); err != nil {
				logger.Errorf("proxy manager stop failed %s", err)
			}
		}

		if !cd.Option.KeepStorage {
			logger.Infof("keep storage disabled")
			cd.StorageManager.CleanUp()
		}

		if cd.dynconfig != nil {
			if err := cd.dynconfig.Stop(); err != nil {
				logger.Errorf("dynconfig client closed failed %s", err)
			}
			logger.Info("dynconfig client closed")
		}

		if cd.managerClient != nil {
			if err := cd.managerClient.Close(); err != nil {
				logger.Errorf("manager client failed to stop: %s", err.Error())
			}
			logger.Info("manager client closed")
		}
	})
}

func (cd *clientDaemon) OnNotify(data *config.DynconfigData) {
	ips := getSchedulerIPs(data.Schedulers)
	if reflect.DeepEqual(cd.schedulers, data.Schedulers) {
		logger.Debugf("scheduler addresses deep equal: %v, used: %#v",
			ips, cd.schedulerClient.GetState())
		return
	}

	// Get the available scheduler addresses and use ip first
	addrs := schedulersToAvailableNetAddrs(data.Schedulers)

	// Update scheduler client addresses
	cd.schedulerClient.UpdateState(addrs)
	cd.schedulers = data.Schedulers

	logger.Infof("scheduler addresses have been updated: %#v", addrs)
}

// getSchedulerIPs gets ips by schedulers.
func getSchedulerIPs(schedulers []*manager.Scheduler) []string {
	ips := []string{}
	for _, scheduler := range schedulers {
		ips = append(ips, scheduler.Ip)
	}

	return ips
}

// schedulersToAvailableNetAddrs coverts []*manager.Scheduler to available []dfnet.NetAddr.
func schedulersToAvailableNetAddrs(schedulers []*manager.Scheduler) []dfnet.NetAddr {
	var schedulerClusterID uint64
	netAddrs := make([]dfnet.NetAddr, 0, len(schedulers))
	for _, scheduler := range schedulers {
		// Check whether scheduler is in the same cluster
		if schedulerClusterID != 0 && schedulerClusterID != scheduler.SchedulerClusterId {
			continue
		}

		// Check whether the ip can be reached
		ipReachable := reachable.New(&reachable.Config{Address: fmt.Sprintf("%s:%d", scheduler.Ip, scheduler.Port)})
		if err := ipReachable.Check(); err != nil {
			logger.Warnf("scheduler address %s:%d is unreachable", scheduler.Ip, scheduler.Port)
		} else {
			schedulerClusterID = scheduler.SchedulerClusterId
			netAddrs = append(netAddrs, dfnet.NetAddr{
				Type: dfnet.TCP,
				Addr: fmt.Sprintf("%s:%d", scheduler.Ip, scheduler.Port),
			})
			continue
		}

		// Check whether the host can be reached
		hostReachable := reachable.New(&reachable.Config{Address: fmt.Sprintf("%s:%d", scheduler.HostName, scheduler.Port)})
		if err := hostReachable.Check(); err != nil {
			logger.Warnf("scheduler address %s:%d is unreachable", scheduler.HostName, scheduler.Port)
		} else {
			schedulerClusterID = scheduler.SchedulerClusterId
			netAddrs = append(netAddrs, dfnet.NetAddr{
				Type: dfnet.TCP,
				Addr: fmt.Sprintf("%s:%d", scheduler.HostName, scheduler.Port),
			})
		}
	}

	return netAddrs
}

// announceSeedPeer announces seed peer to manager.
func (cd *clientDaemon) announceSeedPeer() error {
	if _, err := cd.managerClient.UpdateSeedPeer(&manager.UpdateSeedPeerRequest{
		SourceType:        manager.SourceType_SEED_PEER_SOURCE,
		HostName:          cd.Option.Host.Hostname,
		Type:              cd.Option.Scheduler.Manager.SeedPeer.Type,
		Idc:               cd.Option.Host.IDC,
		NetTopology:       cd.Option.Host.NetTopology,
		Location:          cd.Option.Host.Location,
		Ip:                cd.Option.Host.AdvertiseIP,
		Port:              cd.schedPeerHost.RpcPort,
		DownloadPort:      cd.schedPeerHost.DownPort,
		SeedPeerClusterId: uint64(cd.Option.Scheduler.Manager.SeedPeer.ClusterID),
	}); err != nil {
		return err
	}

	return nil
}

func (cd *clientDaemon) ExportTaskManager() peer.TaskManager {
	return cd.PeerTaskManager
}

func (cd *clientDaemon) ExportPeerHost() *scheduler.PeerHost {
	return cd.schedPeerHost
}
