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
	"net"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/scheduler"
)

type PeerHost interface {
	Serve() error
	GracefulStop()
}

type peerHost struct {
	started chan bool
	host    *scheduler.PeerHost

	Option PeerHostOption

	DownloadManager DownloadManager
	PeerTaskManager PeerTaskManager
	PieceManager    PieceManager
	UploadManager   UploadManager
	StorageManager  StorageManager
}

type PeerHostOption struct {
	Scheduler []string

	Download DownloadOption
	Upload   UploadOption
}

type DownloadOption struct {
	GRPCListen  string
	GRPCNetwork string
	GRPInsecure bool

	ProxyListen   string
	ProxyNetwork  string
	ProxyInsecure bool
}

type UploadOption struct {
	Listen   string
	Network  string
	Insecure bool
}

func NewPeerHost(option PeerHostOption) (PeerHost, error) {
	// TODO initialize peer host information
	host := &scheduler.PeerHost{}

	storageManager, err := NewStorageManager("/tmp/dfdaemon")
	if err != nil {
		return nil, err
	}

	pieceManager, err := NewPieceManager(storageManager)
	if err != nil {
		return nil, err
	}

	// TODO schedulerPeerTaskClient locator
	peerTaskManager, err := NewPeerTaskManager(pieceManager, storageManager, nil)
	if err != nil {
		return nil, err
	}

	downloadManager, err := NewDownloadManager(host, peerTaskManager)
	if err != nil {
		return nil, err
	}

	uploadManager, err := NewUploadManager(storageManager)
	if err != nil {
		return nil, err
	}

	return &peerHost{
		started: make(chan bool),
		host:    host,
		Option:  option,

		DownloadManager: downloadManager,
		PeerTaskManager: peerTaskManager,
		PieceManager:    pieceManager,
		UploadManager:   uploadManager,
		StorageManager:  storageManager,
	}, nil
}

func (ph *peerHost) Serve() error {
	g := errgroup.Group{}
	// serve grpc service
	g.Go(func() error {
		var listener net.Listener
		var err error
		if ph.Option.Download.GRPInsecure {
			listener, err = net.Listen(ph.Option.Download.GRPCNetwork, ph.Option.Download.GRPCListen)
			if err != nil {
				logrus.Errorf("failed to listen for download grpc service: %v", err)
				return err
			}
		} else {
			// TODO tls config
			panic("implement me")
		}
		if err = ph.DownloadManager.ServeGRPC(listener); err != nil {
			logrus.Errorf("failed to serve for download grpc service: %v", err)
			return err
		}
		return nil
	})

	// serve proxy service
	g.Go(func() error {
		var listener net.Listener
		var err error
		if ph.Option.Download.ProxyInsecure {
			listener, err = net.Listen(ph.Option.Download.ProxyNetwork, ph.Option.Download.ProxyListen)
			if err != nil {
				logrus.Errorf("failed to listen for download proxy service: %v", err)
				return err
			}
		} else {
			// TODO tls config
			panic("implement me")
		}
		if err = ph.DownloadManager.ServeProxy(listener); err != nil {
			logrus.Errorf("failed to serve for download proxy service: %v", err)
			return err
		}
		return nil
	})

	// serve upload service
	g.Go(func() error {
		var listener net.Listener
		var err error
		if ph.Option.Upload.Insecure {
			listener, err = net.Listen(ph.Option.Upload.Network, ph.Option.Upload.Listen)
			if err != nil {
				logrus.Errorf("failed to listen for upload service: %v", err)
				return err
			}
		} else {
			// TODO tls config
			panic("implement me")
		}
		if err = ph.UploadManager.Serve(listener); err != nil {
			logrus.Errorf("failed to serve for upload service: %v", err)
			return err
		}
		return nil
	})

	return g.Wait()
}

func (ph *peerHost) GracefulStop() {
	ph.DownloadManager.Stop()
	ph.UploadManager.Stop()
}
