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
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/johanbrandhorst/certify"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	zapadapter "logur.dev/adapter/zap"

	commonv1 "d7y.io/api/pkg/apis/common/v1"
	managerv1 "d7y.io/api/pkg/apis/manager/v1"
	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/gc"
	"d7y.io/dragonfly/v2/client/daemon/metrics"
	"d7y.io/dragonfly/v2/client/daemon/objectstorage"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	"d7y.io/dragonfly/v2/client/daemon/proxy"
	"d7y.io/dragonfly/v2/client/daemon/rpcserver"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/client/daemon/upload"
	"d7y.io/dragonfly/v2/client/util"
	"d7y.io/dragonfly/v2/cmd/dependency"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	internaldynconfig "d7y.io/dragonfly/v2/internal/dynconfig"
	"d7y.io/dragonfly/v2/pkg/cache"
	"d7y.io/dragonfly/v2/pkg/dfnet"
	"d7y.io/dragonfly/v2/pkg/dfpath"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/issuer"
	"d7y.io/dragonfly/v2/pkg/net/ip"
	"d7y.io/dragonfly/v2/pkg/rpc"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/types"
)

type Daemon interface {
	Serve() error
	Stop()

	// ExportTaskManager returns the underlay peer.TaskManager for downloading when embed dragonfly in custom binary
	ExportTaskManager() peer.TaskManager
	// ExportPeerHost returns the underlay schedulerv1.PeerHost for scheduling
	ExportPeerHost() *schedulerv1.PeerHost
}

type clientDaemon struct {
	once *sync.Once
	done chan bool

	schedPeerHost *schedulerv1.PeerHost

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
	managerClient   managerclient.Client
	schedulerClient schedulerclient.Client
	certifyClient   *certify.Certify
}

func New(opt *config.DaemonOption, d dfpath.Dfpath) (Daemon, error) {
	// update plugin directory
	source.UpdatePluginDir(d.PluginDir())

	host := &schedulerv1.PeerHost{
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
		dynconfig      config.Dynconfig
		managerClient  managerclient.Client
		certifyClient  *certify.Certify
		defaultPattern = config.ConvertPattern(opt.Download.DefaultPattern, commonv1.Pattern_P2P)
	)

	if opt.Scheduler.Manager.Enable {
		var (
			grpcCredentials credentials.TransportCredentials
			err             error
		)

		if opt.Security.CACert == "" {
			grpcCredentials = insecure.NewCredentials()
		} else {
			grpcCredentials, err = loadManagerGPRCTLSCredentials(opt.Security)
			if err != nil {
				return nil, err
			}
		}

		managerClient, err = managerclient.GetClientByAddr(
			context.Background(), opt.Scheduler.Manager.NetAddrs, grpc.WithTransportCredentials(grpcCredentials))
		if err != nil {
			return nil, err
		}

		if opt.Security.AutoIssueCert {
			certifyClient = &certify.Certify{
				CommonName:   ip.IPv4,
				Issuer:       issuer.NewDragonflyIssuer(managerClient, issuer.WithValidityPeriod(opt.Security.CertSpec.ValidityPeriod)),
				RenewBefore:  time.Hour,
				CertConfig:   nil,
				IssueTimeout: 0,
				Logger:       zapadapter.New(logger.CoreLogger.Desugar()),
				Cache: cache.NewCertifyMutliCache(
					certify.NewMemCache(),
					certify.DirCache(path.Join(d.CacheDir(), cache.CertifyCacheDirName, types.DfdaemonName))),
			}

			// issue a certificate to reduce first time delay
			if _, err := certifyClient.GetCertificate(&tls.ClientHelloInfo{
				ServerName: ip.IPv4,
			}); err != nil {
				logger.Errorf("issue certificate error: %s", err.Error())
				return nil, err
			}
		}

		// New dynconfig manager client.
		dynconfig, err = config.NewDynconfig(
			config.ManagerSourceType, opt,
			config.WithManagerClient(managerClient),
			config.WithCacheDir(filepath.Join(d.CacheDir(), internaldynconfig.CacheDirName)),
			config.WithExpireTime(opt.Scheduler.Manager.RefreshInterval),
		)
		if err != nil {
			return nil, err
		}
	} else {
		// New dynconfig local client.
		var err error
		dynconfig, err = config.NewDynconfig(config.LocalSourceType, opt)
		if err != nil {
			return nil, err
		}
	}

	var (
		grpcCredentials credentials.TransportCredentials
		err             error
	)

	if certifyClient == nil {
		grpcCredentials = insecure.NewCredentials()
	} else {
		grpcCredentials, err = loadGlobalGPRCTLSCredentials(certifyClient, opt.Security)
		if err != nil {
			return nil, err
		}
	}

	sched, err := schedulerclient.GetClient(context.Background(), dynconfig, grpc.WithTransportCredentials(grpcCredentials))
	if err != nil {
		return nil, fmt.Errorf("failed to get schedulers: %w", err)
	}

	// Storage.Option.DataPath is same with Daemon DataDir
	opt.Storage.DataPath = d.DataDir()
	gcCallback := func(request storage.CommonTaskRequest) {
		er := sched.LeaveTask(context.Background(), &schedulerv1.PeerTarget{
			TaskId: request.TaskID,
			PeerId: request.PeerID,
		})
		if er != nil {
			logger.Errorf("step 4: leave task %s/%s, error: %v", request.TaskID, request.PeerID, er)
		} else {
			logger.Infof("step 4: leave task %s/%s state ok", request.TaskID, request.PeerID)
		}
	}
	storageManager, err := storage.NewStorageManager(opt.Storage.StoreStrategy, &opt.Storage,
		gcCallback, storage.WithGCInterval(opt.GCInterval.Duration))
	if err != nil {
		return nil, err
	}

	pmOpts := []peer.PieceManagerOption{
		peer.WithLimiter(rate.NewLimiter(opt.Download.TotalRateLimit.Limit, int(opt.Download.TotalRateLimit.Limit))),
		peer.WithCalculateDigest(opt.Download.CalculateDigest),
		peer.WithTransportOption(opt.Download.Transport),
		peer.WithConcurrentOption(opt.Download.Concurrent),
	}

	if opt.Download.SyncPieceViaHTTPS && opt.Scheduler.Manager.Enable {
		pmOpts = append(pmOpts, peer.WithSyncPieceViaHTTPS(string(opt.Security.CACert)))
	}

	pieceManager, err := peer.NewPieceManager(opt.Download.PieceDownloadTimeout, pmOpts...)
	if err != nil {
		return nil, err
	}

	peerTaskManagerOption := &peer.TaskManagerOption{
		TaskOption: peer.TaskOption{
			PeerHost:        host,
			SchedulerOption: opt.Scheduler,
			PieceManager:    pieceManager,
			StorageManager:  storageManager,
			WatchdogTimeout: opt.Download.WatchdogTimeout,
			CalculateDigest: opt.Download.CalculateDigest,
			GRPCCredentials: grpcCredentials,
			GRPCDialTimeout: opt.Download.GRPCDialTimeout,
		},
		SchedulerClient:   sched,
		PerPeerRateLimit:  opt.Download.PerPeerRateLimit.Limit,
		TotalRateLimit:    opt.Download.TotalRateLimit.Limit,
		TrafficShaperType: opt.Download.TrafficShaperType,
		Multiplex:         opt.Storage.Multiplex,
		Prefetch:          opt.Download.Prefetch,
		GetPiecesMaxRetry: opt.Download.GetPiecesMaxRetry,
	}
	peerTaskManager, err := peer.NewPeerTaskManager(peerTaskManagerOption)
	if err != nil {
		return nil, err
	}

	// TODO(jim): more server options
	var downloadServerOption []grpc.ServerOption
	if !opt.Download.DownloadGRPC.Security.Insecure || certifyClient != nil {
		tlsCredentials, err := loadLegacyGPRCTLSCredentials(opt.Download.DownloadGRPC.Security, certifyClient, opt.Security)
		if err != nil {
			return nil, err
		}
		downloadServerOption = append(downloadServerOption, grpc.Creds(tlsCredentials))
	}
	var peerServerOption []grpc.ServerOption
	if !opt.Download.PeerGRPC.Security.Insecure || certifyClient != nil {
		tlsCredentials, err := loadLegacyGPRCTLSCredentials(opt.Download.PeerGRPC.Security, certifyClient, opt.Security)
		if err != nil {
			return nil, err
		}
		peerServerOption = append(peerServerOption, grpc.Creds(tlsCredentials))
	}

	rpcManager, err := rpcserver.New(host, peerTaskManager, storageManager, defaultPattern, downloadServerOption, peerServerOption)
	if err != nil {
		return nil, err
	}

	proxyManager, err := proxy.NewProxyManager(host, peerTaskManager, defaultPattern, opt.Proxy)
	if err != nil {
		return nil, err
	}

	uploadOpts := []upload.Option{
		upload.WithLimiter(rate.NewLimiter(opt.Upload.RateLimit.Limit, int(opt.Upload.RateLimit.Limit))),
	}

	if opt.Security.AutoIssueCert && opt.Scheduler.Manager.Enable {
		uploadOpts = append(uploadOpts, upload.WithCertify(certifyClient))
	}

	uploadManager, err := upload.NewUploadManager(opt, storageManager, d.LogDir(), uploadOpts...)
	if err != nil {
		return nil, err
	}

	var objectStorage objectstorage.ObjectStorage
	if opt.ObjectStorage.Enable {
		objectStorage, err = objectstorage.New(opt, dynconfig, peerTaskManager, storageManager, d.LogDir())
		if err != nil {
			return nil, err
		}
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
		managerClient:   managerClient,
		schedulerClient: sched,
		certifyClient:   certifyClient,
	}, nil
}

func loadLegacyGPRCTLSCredentials(opt config.SecurityOption, certifyClient *certify.Certify, security config.GlobalSecurityOption) (credentials.TransportCredentials, error) {
	// merge all options
	var mergedOptions = security
	mergedOptions.CACert += "\n" + opt.CACert
	mergedOptions.TLSVerify = opt.TLSVerify || security.TLSVerify

	if opt.TLSConfig == nil {
		opt.TLSConfig = &tls.Config{}
	}

	// Load server's certificate and private key
	var (
		serverCert tls.Certificate
		err        error
	)
	if certifyClient == nil {
		serverCert, err = tls.X509KeyPair([]byte(opt.Cert), []byte(opt.Key))
		if err != nil {
			return nil, err
		}
	}

	options := []func(c *tls.Config){
		func(c *tls.Config) {
			if certifyClient == nil {
				c.Certificates = []tls.Certificate{serverCert}
			} else {
				// enable auto issue certificate
				c.GetCertificate = config.GetCertificate(certifyClient)
				c.GetClientCertificate = certifyClient.GetClientCertificate
			}
		},
	}

	return loadGPRCTLSCredentialsWithOptions(opt.TLSConfig, security, options...)
}

func loadGlobalGPRCTLSCredentials(certifyClient *certify.Certify, security config.GlobalSecurityOption) (credentials.TransportCredentials, error) {
	return loadGPRCTLSCredentialsWithOptions(nil, security, func(c *tls.Config) {
		c.GetCertificate = config.GetCertificate(certifyClient)
		c.GetClientCertificate = certifyClient.GetClientCertificate
	})
}

func loadManagerGPRCTLSCredentials(security config.GlobalSecurityOption) (credentials.TransportCredentials, error) {
	return loadGPRCTLSCredentialsWithOptions(nil, security, func(c *tls.Config) {
		c.ClientAuth = tls.NoClientCert
	})
}

func loadGPRCTLSCredentialsWithOptions(baseConfig *tls.Config, security config.GlobalSecurityOption,
	opts ...func(c *tls.Config)) (credentials.TransportCredentials, error) {
	certPool := x509.NewCertPool()

	if security.CACert == "" {
		return nil, fmt.Errorf("empty glocal CA's certificate")
	}

	if !certPool.AppendCertsFromPEM([]byte(security.CACert)) {
		return nil, fmt.Errorf("failed to add global CA's certificate")
	}

	var tlsConfig *tls.Config
	if baseConfig == nil {
		tlsConfig = &tls.Config{}
	} else {
		tlsConfig = baseConfig.Clone()
	}

	tlsConfig.RootCAs = certPool
	tlsConfig.ClientCAs = certPool

	if security.TLSVerify {
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	for _, opt := range opts {
		opt(tlsConfig)
	}

	switch security.TLSPolicy {
	case rpc.DefaultTLSPolicy, rpc.PreferTLSPolicy:
		return rpc.NewMuxTransportCredentials(tlsConfig,
			rpc.WithTLSPreferClientHandshake(security.TLSPolicy == rpc.PreferTLSPolicy)), nil
	case rpc.ForceTLSPolicy:
		return credentials.NewTLS(tlsConfig), nil
	default:
		return nil, fmt.Errorf("invalid tlsPolicy: %s", security.TLSPolicy)
	}
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
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM([]byte(opt.Security.CACert))
		tlsConfig.ClientCAs = caCertPool
		if opt.Security.TLSVerify {
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		}
	}

	tlsConfig.Certificates = make([]tls.Certificate, 1)
	tlsConfig.Certificates[0], err = tls.X509KeyPair([]byte(opt.Security.Cert), []byte(opt.Security.Key))
	if err != nil {
		return nil, -1, err
	}

	return tls.NewListener(ln, tlsConfig), port, nil
}

func (cd *clientDaemon) Serve() error {
	var (
		watchers []func(daemon *config.DaemonOption)
		interval = cd.Option.Reload.Interval.Duration
	)
	cd.GCManager.Start()
	// prepare download service listen
	if cd.Option.Download.DownloadGRPC.UnixListen == nil {
		return errors.New("download grpc unix listen option is empty")
	}
	_ = os.Remove(cd.dfpath.DaemonSockPath())
	downloadListener, err := rpc.Listen(dfnet.NetAddr{
		Type: dfnet.UNIX,
		Addr: cd.dfpath.DaemonSockPath(),
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
	var objectStorageListener net.Listener
	if cd.Option.ObjectStorage.Enable {
		if cd.Option.ObjectStorage.TCPListen == nil {
			return errors.New("object storage tcp listen option is empty")
		}
		objectStorageListener, _, err = cd.prepareTCPListener(cd.Option.ObjectStorage.ListenOption, true)
		if err != nil {
			logger.Errorf("failed to listen for object storage service: %v", err)
			return err
		}
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
		watchers = append(watchers, func(daemon *config.DaemonOption) {
			cd.ProxyManager.Watch(daemon.Proxy)
		})
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
	if cd.Option.ObjectStorage.Enable {
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
	}

	// enable seed peer mode
	if cd.managerClient != nil && cd.Option.Scheduler.Manager.SeedPeer.Enable {
		logger.Info("announce to manager")
		if err := cd.announceSeedPeer(); err != nil {
			return err
		}

		go func() {
			logger.Info("start keepalive to manager")
			cd.managerClient.KeepAlive(cd.Option.Scheduler.Manager.SeedPeer.KeepAlive.Interval, &managerv1.KeepAliveRequest{
				SourceType: managerv1.SourceType_SEED_PEER_SOURCE,
				HostName:   cd.Option.Host.Hostname,
				Ip:         cd.Option.Host.AdvertiseIP,
				ClusterId:  uint64(cd.Option.Scheduler.Manager.SeedPeer.ClusterID),
			})
		}()
	}

	if cd.Option.AliveTime.Duration > 0 {
		g.Go(func() error {
			for {
				select {
				case <-time.After(cd.Option.AliveTime.Duration):
					var keepalives = []util.KeepAlive{
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
	if cd.Option.Scheduler.Manager.Enable {
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

	if len(watchers) > 0 && interval > 0 {
		go func() {
			dependency.WatchConfig(interval, func() any {
				return config.NewDaemonConfig()
			}, func(cfg any) {
				daemonConfig := cfg.(*config.DaemonOption)
				for _, w := range watchers {
					w(daemonConfig)
				}
			})
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

		if err := cd.PeerTaskManager.Stop(context.Background()); err != nil {
			logger.Errorf("peertask manager stop failed %s", err)
		}

		if cd.Option.ObjectStorage.Enable {
			if err := cd.ObjectStorage.Stop(); err != nil {
				logger.Errorf("object storage stop failed %s", err)
			}
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

		if cd.Option.Scheduler.Manager.Enable {
			if err := cd.dynconfig.Stop(); err != nil {
				logger.Errorf("dynconfig client closed failed %s", err)
			}
			logger.Info("dynconfig client closed")
		}
	})
}

// announceSeedPeer announces seed peer to manager.
func (cd *clientDaemon) announceSeedPeer() error {
	var objectStoragePort int32
	if cd.Option.ObjectStorage.Enable {
		objectStoragePort = int32(cd.Option.ObjectStorage.TCPListen.PortRange.Start)
	}

	if _, err := cd.managerClient.UpdateSeedPeer(context.Background(), &managerv1.UpdateSeedPeerRequest{
		SourceType:        managerv1.SourceType_SEED_PEER_SOURCE,
		HostName:          cd.Option.Host.Hostname,
		Type:              cd.Option.Scheduler.Manager.SeedPeer.Type,
		Idc:               cd.Option.Host.IDC,
		NetTopology:       cd.Option.Host.NetTopology,
		Location:          cd.Option.Host.Location,
		Ip:                cd.Option.Host.AdvertiseIP,
		Port:              cd.schedPeerHost.RpcPort,
		DownloadPort:      cd.schedPeerHost.DownPort,
		ObjectStoragePort: objectStoragePort,
		SeedPeerClusterId: uint64(cd.Option.Scheduler.Manager.SeedPeer.ClusterID),
	}); err != nil {
		return err
	}

	return nil
}

func (cd *clientDaemon) ExportTaskManager() peer.TaskManager {
	return cd.PeerTaskManager
}

func (cd *clientDaemon) ExportPeerHost() *schedulerv1.PeerHost {
	return cd.schedPeerHost
}
