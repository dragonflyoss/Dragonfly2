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

package rpcserver

import (
	"crypto/tls"
	"crypto/x509"
	"time"

	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc"
	"gorm.io/gorm"

	managerv1 "d7y.io/api/pkg/apis/manager/v1"

	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/metrics"
	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/searcher"
	pkgcache "d7y.io/dragonfly/v2/pkg/cache"
	"d7y.io/dragonfly/v2/pkg/objectstorage"
	managerserver "d7y.io/dragonfly/v2/pkg/rpc/manager/server"
)

const (
	// DefaultPeerCacheExpiration is default expiration of peer cache.
	DefaultPeerCacheExpiration = 10 * time.Minute

	// DefaultPeerCacheCleanupInterval is default cleanup interval of peer cache.
	DefaultPeerCacheCleanupInterval = 1 * time.Minute
)

// SelfSignedCert is self signed certificate.
type SelfSignedCert struct {
	// TLSCert is certificate of tls.
	TLSCert *tls.Certificate

	// X509Cert is certificate of x509.
	X509Cert *x509.Certificate

	// CertChain is certificate chain of ASN.1 DER form.
	CertChain [][]byte
}

// Server is grpc server.
type Server struct {
	// Manager configuration.
	config *config.Config

	// GORM instance.
	db *gorm.DB

	// Redis universal client interface.
	rdb redis.UniversalClient

	// Cache instance.
	cache *cache.Cache

	// Peer memory cache.
	peerCache pkgcache.Cache

	// Searcher interface.
	searcher searcher.Searcher

	// Object storage interface.
	objectStorage objectstorage.ObjectStorage

	// Object storage configuration.
	objectStorageConfig *config.ObjectStorageConfig

	// serverOptions is server options of grpc.
	serverOptions []grpc.ServerOption

	// selfSignedCert is self signed certificate.
	selfSignedCert *SelfSignedCert
}

// Option is a functional option for rpc server.
type Option func(s *Server) error

// WithCertificate set the self signed certificate for server.
func WithSelfSignedCert(tlsCert *tls.Certificate) Option {
	return func(s *Server) error {
		x509CACert, err := x509.ParseCertificate(tlsCert.Certificate[0])
		if err != nil {
			return err
		}

		s.selfSignedCert = &SelfSignedCert{
			TLSCert:   tlsCert,
			X509Cert:  x509CACert,
			CertChain: tlsCert.Certificate,
		}

		return nil
	}
}

// WithGRPCServerOptions set the server options of grpc.
func WithGRPCServerOptions(opts []grpc.ServerOption) Option {
	return func(s *Server) error {
		s.serverOptions = opts
		return nil
	}
}

// New returns a new manager server from the given options.
func New(
	cfg *config.Config, database *database.Database, cache *cache.Cache, searcher searcher.Searcher,
	objectStorage objectstorage.ObjectStorage, objectStorageConfig *config.ObjectStorageConfig, opts ...Option,
) (*Server, *grpc.Server, error) {
	s := &Server{
		config:              cfg,
		db:                  database.DB,
		rdb:                 database.RDB,
		cache:               cache,
		peerCache:           pkgcache.New(DefaultPeerCacheExpiration, DefaultPeerCacheCleanupInterval),
		searcher:            searcher,
		objectStorage:       objectStorage,
		objectStorageConfig: objectStorageConfig,
	}

	// Peer cache is evicted, and the metrics of the peer should be released.
	s.peerCache.OnEvicted(func(k string, v any) {
		if req, ok := v.(*managerv1.ListSchedulersRequest); ok {
			metrics.PeerGauge.WithLabelValues(req.Version, req.Commit).Dec()
		}
	})

	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, nil, err
		}
	}

	return s, managerserver.New(
		newManagerServerV1(s.config, database, s.cache, s.peerCache, s.searcher, s.objectStorage, s.objectStorageConfig),
		newManagerServerV2(s.config, database, s.cache, s.peerCache, s.searcher, s.objectStorage, s.objectStorageConfig),
		newSecurityServerV1(s.selfSignedCert),
		s.serverOptions...), nil
}

// Get scheduler cluster names.
func getSchedulerClusterNames(clusters []models.SchedulerCluster) []string {
	names := []string{}
	for _, cluster := range clusters {
		names = append(names, cluster.Name)
	}

	return names
}
