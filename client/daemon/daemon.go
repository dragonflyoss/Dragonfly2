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
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	zapadapter "logur.dev/adapter/zap"

	schedulerv1 "d7y.io/api/v2/pkg/apis/scheduler/v1"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/announcer"
	"d7y.io/dragonfly/v2/client/daemon/gc"
	"d7y.io/dragonfly/v2/client/daemon/metrics"
	"d7y.io/dragonfly/v2/client/daemon/networktopology"
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
	securityclient "d7y.io/dragonfly/v2/pkg/rpc/security/client"
	"d7y.io/dragonfly/v2/pkg/source"
	_ "d7y.io/dragonfly/v2/pkg/source/loader" // register all source clients
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
	managerClient   managerclient.V1
	securityClient  securityclient.V1
	schedulerClient schedulerclient.V1
	certifyClient   *certify.Certify
	announcer       announcer.Announcer
	networkTopology networktopology.NetworkTopology
}

func New(opt *config.DaemonOption, d dfpath.Dfpath) (Daemon, error) {
	// update plugin directory
	source.UpdatePluginDir(d.PluginDir())

	// FIXME the viper casts all case sensitive keys into lower case, but the resource clients option is map[string]interface{}, it should not be casted.
	// issue: https://github.com/spf13/viper/issues/1014
	tmpOpt := config.NewDaemonConfig()
	err := tmpOpt.Load(viper.ConfigFileUsed())
	if err != nil {
		return nil, fmt.Errorf("read config error: %w", err)
	}

	err = source.InitSourceClients(tmpOpt.Download.ResourceClients)
	if err != nil {
		return nil, err
	}

	host := &schedulerv1.PeerHost{
		Id:       idgen.HostIDV2(opt.Host.AdvertiseIP.String(), opt.Host.Hostname),
		Ip:       opt.Host.AdvertiseIP.String(),
		RpcPort:  int32(opt.Download.PeerGRPC.TCPListen.PortRange.Start),
		DownPort: 0,
		Hostname: opt.Host.Hostname,
		Location: opt.Host.Location,
		Idc:      opt.Host.IDC,
	}

	var (
		managerClient  managerclient.V1
		securityClient securityclient.V1
		certifyClient  *certify.Certify
	)

	if opt.Scheduler.Manager.Enable {
		var grpcCredentials credentials.TransportCredentials

		if opt.Security.CACert == "" {
			grpcCredentials = insecure.NewCredentials()
		} else {
			grpcCredentials, err = loadManagerGPRCTLSCredentials(opt.Security)
			if err != nil {
				return nil, err
			}
		}

		managerClient, err = managerclient.GetV1ByNetAddrs(
			context.Background(), opt.Scheduler.Manager.NetAddrs, grpc.WithTransportCredentials(grpcCredentials))
		if err != nil {
			return nil, err
		}

		if opt.Security.AutoIssueCert {
			// Initialize security client.
			securityClient, err = securityclient.GetV1ByAddr(context.Background(), opt.Scheduler.Manager.NetAddrs, grpc.WithTransportCredentials(grpcCredentials))
			if err != nil {
				return nil, err
			}

			certifyClient = &certify.Certify{
				CommonName:  ip.IPv4.String(),
				Issuer:      issuer.NewDragonflyIssuer(securityClient, issuer.WithValidityPeriod(opt.Security.CertSpec.ValidityPeriod)),
				RenewBefore: time.Hour,
				CertConfig: &certify.CertConfig{
					SubjectAlternativeNames:   opt.Security.CertSpec.DNSNames,
					IPSubjectAlternativeNames: append(opt.Security.CertSpec.IPAddresses, opt.Host.AdvertiseIP),
				},
				IssueTimeout: 0,
				Logger:       zapadapter.New(logger.CoreLogger.Desugar()),
				Cache: cache.NewCertifyMutliCache(
					certify.NewMemCache(),
					certify.DirCache(path.Join(d.CacheDir(), cache.CertifyCacheDirName, types.DfdaemonName))),
			}

			// issue a certificate to reduce first time delay
			if _, err = certifyClient.GetCertificate(&tls.ClientHelloInfo{
				ServerName: ip.IPv4.String(),
			}); err != nil {
				logger.Errorf("issue certificate error: %s", err.Error())
				return nil, err
			}
		}
	}

	var grpcCredentials credentials.TransportCredentials
	if certifyClient == nil {
		grpcCredentials = insecure.NewCredentials()
	} else {
		grpcCredentials, err = loadGlobalGPRCTLSCredentials(certifyClient, opt.Security)
		if err != nil {
			return nil, err
		}
	}

	// New dynconfig manager client.
	var dynconfig config.Dynconfig
	if managerClient != nil {
		dynconfig, err = config.NewDynconfig(
			config.ManagerSourceType, opt,
			config.WithManagerClient(managerClient),
			config.WithCacheDir(filepath.Join(d.CacheDir(), internaldynconfig.CacheDirName)),
			config.WithTransportCredentials(grpcCredentials),
		)
		if err != nil {
			return nil, err
		}
	} else {
		dynconfig, err = config.NewDynconfig(config.LocalSourceType, opt, config.WithTransportCredentials(grpcCredentials))
		if err != nil {
			return nil, err
		}
	}

	schedulerClient, err := schedulerclient.GetV1(context.Background(), dynconfig, grpc.WithTransportCredentials(grpcCredentials))
	if err != nil {
		return nil, fmt.Errorf("failed to get schedulers: %w", err)
	}

	// Storage.Option.DataPath is same with Daemon DataDir
	opt.Storage.DataPath = d.DataDir()
	gcCallback := func(request storage.CommonTaskRequest) {
		er := schedulerClient.LeaveTask(context.Background(), &schedulerv1.PeerTarget{
			TaskId: request.TaskID,
			PeerId: request.PeerID,
		})
		if er != nil {
			logger.Errorf("step 4: leave task %s/%s, error: %v", request.TaskID, request.PeerID, er)
		} else {
			logger.Infof("step 4: leave task %s/%s state ok", request.TaskID, request.PeerID)
		}
	}
	dirMode := os.FileMode(opt.DataDirMode)
	storageManager, err := storage.NewStorageManager(opt.Storage.StoreStrategy, &opt.Storage,
		gcCallback, dirMode, storage.WithGCInterval(opt.GCInterval.Duration))
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
		SchedulerClient:   schedulerClient,
		PerPeerRateLimit:  opt.Download.PerPeerRateLimit.Limit,
		TotalRateLimit:    opt.Download.TotalRateLimit.Limit,
		TrafficShaperType: opt.Download.TrafficShaperType,
		Multiplex:         opt.Storage.Multiplex,
		Prefetch:          opt.Download.Prefetch,
		GetPiecesMaxRetry: opt.Download.GetPiecesMaxRetry,
		SplitRunningTasks: opt.Download.SplitRunningTasks,
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

	rpcManager, err := rpcserver.New(host, peerTaskManager, storageManager,
		opt.Download.RecursiveConcurrent.GoroutineCount, opt.Download.CacheRecursiveMetadata, downloadServerOption, peerServerOption)
	if err != nil {
		return nil, err
	}
	// register notify for health check
	dynconfig.Register(rpcManager)

	proxyManager, err := proxy.NewProxyManager(host, peerTaskManager, opt.Proxy)
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
		securityClient:  securityClient,
		schedulerClient: schedulerClient,
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

var noTLSConfig *tls.Config = nil

func loadGlobalGPRCTLSCredentials(certifyClient *certify.Certify, security config.GlobalSecurityOption) (credentials.TransportCredentials, error) {
	return loadGPRCTLSCredentialsWithOptions(noTLSConfig, security, func(c *tls.Config) {
		c.GetCertificate = config.GetCertificate(certifyClient)
		c.GetClientCertificate = certifyClient.GetClientCertificate
	})
}

func loadManagerGPRCTLSCredentials(security config.GlobalSecurityOption) (credentials.TransportCredentials, error) {
	return loadGPRCTLSCredentialsWithOptions(noTLSConfig, security, func(c *tls.Config) {
		c.ClientAuth = tls.NoClientCert
	})
}

func loadGPRCTLSCredentialsWithOptions(baseConfig *tls.Config, security config.GlobalSecurityOption,
	opts ...func(c *tls.Config)) (credentials.TransportCredentials, error) {
	certPool := x509.NewCertPool()

	if security.CACert == "" {
		return nil, errors.New("empty global CA's certificate")
	}

	if !certPool.AppendCertsFromPEM([]byte(security.CACert)) {
		return nil, errors.New("failed to add global CA's certificate")
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
		tlsConfig.RootCAs = caCertPool
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
	var (
		objectStorageListener net.Listener
		objectStoragePort     int
	)
	if cd.Option.ObjectStorage.Enable {
		if cd.Option.ObjectStorage.TCPListen == nil {
			return errors.New("object storage tcp listen option is empty")
		}
		objectStorageListener, objectStoragePort, err = cd.prepareTCPListener(cd.Option.ObjectStorage.ListenOption, true)
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

	// serve announcer
	var announcerOptions []announcer.Option
	if cd.managerClient != nil {
		announcerOptions = append(announcerOptions, announcer.WithManagerClient(cd.managerClient))
	}

	if cd.Option.ObjectStorage.Enable {
		announcerOptions = append(announcerOptions, announcer.WithObjectStoragePort(int32(objectStoragePort)))
	}

	cd.announcer = announcer.New(&cd.Option, cd.dynconfig, cd.schedPeerHost.Id, cd.schedPeerHost.RpcPort,
		cd.schedPeerHost.DownPort, cd.schedulerClient, announcerOptions...)
	go func() {
		logger.Info("serve announcer")
		if err := cd.announcer.Serve(); err != nil {
			logger.Fatalf("failed to serve for announcer: %v", err)
		}
	}()

	// serve network topology
	if cd.Option.NetworkTopology.Enable {
		cd.networkTopology, err = networktopology.NewNetworkTopology(&cd.Option, cd.schedPeerHost.Id, cd.schedPeerHost.RpcPort, cd.schedPeerHost.DownPort, cd.schedulerClient)
		if err != nil {
			logger.Errorf("failed to create network topology: %v", err)
			return err
		}

		// serve network topology service
		logger.Infof("serve network topology")
		go cd.networkTopology.Serve()
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
	g.Go(func() error {
		if err := cd.dynconfig.Serve(); err != nil {
			logger.Errorf("dynconfig start failed %v", err)
			return err
		}

		logger.Info("dynconfig start successfully")
		return nil
	})

	// when there is no manager configured, watch schedulers in local config
	if cd.managerClient == nil {
		watchers = append(watchers, cd.dynconfig.OnNotify)
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

		if cd.ProxyManager.IsEnabled() {
			if err := cd.ProxyManager.Stop(); err != nil {
				logger.Errorf("proxy manager stop failed %s", err)
			}
		}

		if cd.schedulerClient != nil {
			if !cd.Option.KeepStorage {
				logger.Info("leave host with scheduler client")
				if err := cd.schedulerClient.LeaveHost(context.Background(), &schedulerv1.LeaveHostRequest{Id: cd.schedPeerHost.Id}); err != nil {
					logger.Errorf("leave host with scheduler client failed: %s", err.Error())
				}
			}

			if err := cd.schedulerClient.Close(); err != nil {
				logger.Errorf("scheduler client failed to stop: %s", err.Error())
			} else {
				logger.Info("scheduler client closed")
			}
		}

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

		if !cd.Option.KeepStorage {
			logger.Infof("keep storage disabled")
			cd.StorageManager.CleanUp()
		}

		if err := cd.announcer.Stop(); err != nil {
			logger.Errorf("announcer stop failed %s", err)
		}

		if cd.networkTopology != nil {
			cd.networkTopology.Stop()
		}

		if err := cd.dynconfig.Stop(); err != nil {
			logger.Errorf("dynconfig client closed failed %s", err)
		} else {
			logger.Info("dynconfig client closed")
		}

		if cd.managerClient != nil {
			if err := cd.managerClient.Close(); err != nil {
				logger.Errorf("manager client failed to stop: %s", err.Error())
			} else {
				logger.Info("manager client closed")
			}
		}

		if cd.securityClient != nil {
			if err := cd.securityClient.Close(); err != nil {
				logger.Errorf("security client failed to stop: %s", err.Error())
			} else {
				logger.Info("security client closed")
			}
		}
	})
}

func (cd *clientDaemon) ExportTaskManager() peer.TaskManager {
	return cd.PeerTaskManager
}

func (cd *clientDaemon) ExportPeerHost() *schedulerv1.PeerHost {
	return cd.schedPeerHost
}
