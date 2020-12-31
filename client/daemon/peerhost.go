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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/dragonflyoss/Dragonfly2/client/daemon/gc"
	"github.com/dragonflyoss/Dragonfly2/client/daemon/misc"
	"github.com/dragonflyoss/Dragonfly2/client/daemon/peer"
	"github.com/dragonflyoss/Dragonfly2/client/daemon/rpc"
	"github.com/dragonflyoss/Dragonfly2/client/daemon/storage"
	"github.com/dragonflyoss/Dragonfly2/client/daemon/upload"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
)

type PeerHost interface {
	Serve() error
	Stop()
}

type peerHost struct {
	started           chan bool
	schedulerPeerHost *scheduler.PeerHost

	Option PeerHostOption

	RPCManager     rpc.Manager
	UploadManager  upload.Manager
	StorageManager storage.Manager
	GCManager      gc.Manager

	PeerTaskManager peer.PeerTaskManager
	PieceManager    peer.PieceManager
}

type PeerHostOption struct {
	Scheduler SchedulerOption

	// AliveTime indicates alive duration for which daemon keeps no accessing by any uploading and download requests,
	// after this period daemon will automatically exit
	// when AliveTime == 0, will run infinitely
	// TODO keepalive detect
	AliveTime  time.Duration
	GCInterval time.Duration

	Download DownloadOption
	Upload   UploadOption
	Storage  StorageOption
}

type SchedulerOption struct {
	Addresses   []string
	DialOptions []grpc.DialOption
}

type DownloadOption struct {
	RateLimit rate.Limit
	GRPC      ListenOption
	Proxy     *ListenOption
}

type UploadOption struct {
	ListenOption
	RateLimit rate.Limit
}

type ListenOption struct {
	TLSConfig *tls.Config
	Listen    string
	Network   string
	Insecure  bool

	CACert string
	Cert   string
	Key    string
}

type StorageOption struct {
	storage.Option
	Driver storage.Driver
}

func NewPeerHost(host *scheduler.PeerHost, opt PeerHostOption) (PeerHost, error) {
	storageManager, err := storage.NewStorageManager(opt.Storage.Driver, &opt.Storage.Option)
	if err != nil {
		return nil, err
	}

	pieceManager, err := peer.NewPieceManager(storageManager,
		peer.WithLimiter(rate.NewLimiter(opt.Download.RateLimit, int(opt.Download.RateLimit))))
	if err != nil {
		return nil, err
	}

	// TODO schedulerPeerTaskClient locator
	sched, err := misc.NewStaticSchedulerLocator(opt.Scheduler.Addresses, opt.Scheduler.DialOptions...)
	if err != nil {
		return nil, err
	}
	peerTaskManager, err := peer.NewPeerTaskManager(pieceManager, storageManager, sched)
	if err != nil {
		return nil, err
	}

	var serverOption []grpc.ServerOption
	if !opt.Download.GRPC.Insecure {
		tlsCredentials, err := loadGPRCTLSCredentials(opt.Download.GRPC)
		if err != nil {
			return nil, err
		}
		serverOption = append(serverOption, grpc.Creds(tlsCredentials))
	}
	rpcManager, err := rpc.NewManager(host, peerTaskManager, storageManager, serverOption...)
	if err != nil {
		return nil, err
	}

	uploadManager, err := upload.NewUploadManager(storageManager,
		upload.WithLimiter(rate.NewLimiter(opt.Upload.RateLimit, int(opt.Upload.RateLimit))))
	if err != nil {
		return nil, err
	}

	return &peerHost{
		started:           make(chan bool),
		schedulerPeerHost: host,
		Option:            opt,

		RPCManager:      rpcManager,
		PeerTaskManager: peerTaskManager,
		PieceManager:    pieceManager,
		UploadManager:   uploadManager,
		StorageManager:  storageManager,
		GCManager:       gc.NewManager(opt.GCInterval),
	}, nil
}

func loadGPRCTLSCredentials(opt ListenOption) (credentials.TransportCredentials, error) {
	// Load certificate of the CA who signed client's certificate
	pemClientCA, err := ioutil.ReadFile(opt.CACert)
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

func (ph *peerHost) prepareListener(opt ListenOption) (net.Listener, error) {
	ln, err := net.Listen(opt.Network, opt.Listen)
	if err != nil {
		return nil, err
	}
	if opt.Insecure {
		return ln, err
	}

	// Create the TLS Config with the CA pool and enable Client certificate validation
	if opt.TLSConfig == nil {
		opt.TLSConfig = &tls.Config{}
	}
	tlsConfig := opt.TLSConfig
	if opt.CACert != "" {
		caCert, err := ioutil.ReadFile(opt.CACert)
		if err != nil {
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.ClientCAs = caCertPool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}
	tlsConfig.Certificates = make([]tls.Certificate, 1)
	tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(opt.Cert, opt.Key)
	if err != nil {
		return nil, err
	}

	return tls.NewListener(ln, tlsConfig), nil
}

func (ph *peerHost) Serve() error {
	ph.GCManager.Start()

	g := errgroup.Group{}
	// serve grpc service
	g.Go(func() error {
		logger.Infof("serve download grpc at %s://%s",
			ph.Option.Download.GRPC.Network, ph.Option.Download.GRPC.Listen)
		err := ph.RPCManager.ServeDaemon(ph.Option.Download.GRPC.Network, ph.Option.Download.GRPC.Listen)
		if err != nil {
			logger.Errorf("failed to serve for download grpc service: %v", err)
			return err
		}
		return nil
	})

	// serve proxy service
	if ph.Option.Download.Proxy != nil {
		g.Go(func() error {
			listener, err := ph.prepareListener(*ph.Option.Download.Proxy)
			if err != nil {
				logger.Errorf("failed to listen for download proxy service: %v", err)
				return err
			}
			if err = ph.RPCManager.ServeProxy(listener); err != nil {
				logger.Errorf("failed to serve for download proxy service: %v", err)
				return err
			}
			return nil
		})
	}

	// serve upload service
	g.Go(func() error {
		listener, err := ph.prepareListener(ph.Option.Upload.ListenOption)
		if err != nil {
			logger.Errorf("failed to listen for upload service: %v", err)
			return err
		}
		if err = ph.UploadManager.Serve(listener); err != nil {
			logger.Errorf("failed to serve for upload service: %v", err)
			return err
		}
		return nil
	})

	return g.Wait()
}

func (ph *peerHost) Stop() {
	ph.RPCManager.Stop()
	ph.UploadManager.Stop()
}
