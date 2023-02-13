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

//go:generate mockgen -destination mocks/upload_manager_mock.go -source upload_manager.go -package mocks

package upload

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"strings"
	"time"

	ginzap "github.com/gin-contrib/zap"
	"github.com/gin-gonic/gin"
	"github.com/go-http-utils/headers"
	"github.com/johanbrandhorst/certify"
	ginprometheus "github.com/mcuadros/go-gin-prometheus"
	"github.com/soheilhy/cmux"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	nethttp "d7y.io/dragonfly/v2/pkg/net/http"
)

const (
	PrometheusSubsystemName = "dragonfly_dfdaemon_upload"
	OtelServiceName         = "dragonfly-dfdaemon-upload"
)

const (
	RouterGroupDownload = "/download"
)

var GinLogFileName = "gin-upload.log"

// Manager is the interface used for upload task.
type Manager interface {
	// Started upload manager server.
	Serve(lis net.Listener) error

	// Stop upload manager server.
	Stop() error
}

// uploadManager provides upload manager function.
type uploadManager struct {
	*http.Server
	*rate.Limiter
	storageManager storage.Manager
	certify        *certify.Certify
}

// Option is a functional option for configuring the upload manager.
type Option func(um *uploadManager)

// WithLimiter sets upload rate limiter, the burst size must be bigger than piece size.
func WithLimiter(limiter *rate.Limiter) func(*uploadManager) {
	return func(manager *uploadManager) {
		manager.Limiter = limiter
	}
}

func WithCertify(ct *certify.Certify) func(manager *uploadManager) {
	return func(manager *uploadManager) {
		manager.certify = ct
	}
}

// New returns a new Manager instence.
func NewUploadManager(cfg *config.DaemonOption, storageManager storage.Manager, logDir string, opts ...Option) (Manager, error) {
	um := &uploadManager{
		storageManager: storageManager,
	}

	router := um.initRouter(cfg, logDir)
	um.Server = &http.Server{
		Handler: router,
	}

	for _, opt := range opts {
		opt(um)
	}

	return um, nil
}

// Started upload manager server.
func (um *uploadManager) Serve(listener net.Listener) error {
	if um.certify == nil {
		return um.Server.Serve(listener)
	}

	logger.Debugf("use http and https uploader in same listener")
	m := cmux.New(listener)
	httpListener := m.Match(cmux.HTTP1Fast())
	tlsListener := m.Match(cmux.Any())

	go func() {
		if err := um.Server.Serve(httpListener); err != nil {
			logger.Debugf("upload server exit: %s", err)
		}
	}()

	go func() {
		tlsConfig := &tls.Config{
			GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
				// FIXME peers need pure ip cert, certify checks the ServerName, so workaround here
				hello.ServerName = "peer"
				return um.certify.GetCertificate(hello)
			},
		}

		tlsListener = tls.NewListener(tlsListener, tlsConfig)
		if err := um.Server.Serve(tlsListener); err != nil {
			logger.Debugf("upload server exit: %s", err)
		}
	}()
	return m.Serve()
}

// Stop upload manager server.
func (um *uploadManager) Stop() error {
	return um.Server.Shutdown(context.Background())
}

// Initialize router of gin.
func (um *uploadManager) initRouter(cfg *config.DaemonOption, logDir string) *gin.Engine {
	// Set mode
	if !cfg.Verbose {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()

	// Middleware
	r.Use(gin.Recovery())
	r.Use(ginzap.Ginzap(logger.GinLogger.Desugar(), time.RFC3339, true))
	r.Use(ginzap.RecoveryWithZap(logger.GinLogger.Desugar(), true))

	// Prometheus metrics
	p := ginprometheus.NewPrometheus(PrometheusSubsystemName)
	// Prometheus metrics need to reduce label,
	// refer to https://prometheus.io/docs/practices/instrumentation/#do-not-overuse-labels.
	p.ReqCntURLLabelMappingFn = func(c *gin.Context) string {
		if strings.HasPrefix(c.Request.URL.Path, RouterGroupDownload) {
			return RouterGroupDownload
		}

		return c.Request.URL.Path
	}
	p.Use(r)

	// Opentelemetry
	if cfg.Options.Telemetry.Jaeger != "" {
		r.Use(otelgin.Middleware(OtelServiceName))
	}

	// Health Check.
	r.GET("/healthy", um.getHealth)

	// Peer download task.
	d := r.Group(RouterGroupDownload)
	d.GET(":task_prefix/:task_id", um.getDownload)

	return r
}

// getHealth uses to check server health.
func (um *uploadManager) getHealth(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, http.StatusText(http.StatusOK))
}

// getDownload uses to upload a task file when other peers download from it.
func (um *uploadManager) getDownload(ctx *gin.Context) {
	var params DownloadParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var query DownalodQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	taskID := params.TaskID
	peerID := query.PeerID

	log := logger.WithTaskAndPeerID(taskID, peerID).With("component", "uploadManager")
	log.Debugf("upload piece for task %s/%s to %s, request header: %#v", taskID, peerID, ctx.Request.RemoteAddr, ctx.Request.Header)
	rg, err := nethttp.ParseRange(ctx.GetHeader(headers.Range), math.MaxInt64)
	if err != nil {
		log.Errorf("parse range with error: %s", err)
		ctx.JSON(http.StatusBadRequest, gin.H{"errors": err.Error()})
		return
	}

	if len(rg) != 1 {
		log.Error("multi range parsed, not support")
		ctx.JSON(http.StatusBadRequest, gin.H{"errors": "invalid range"})
		return
	}

	reader, closer, err := um.storageManager.ReadPiece(ctx,
		&storage.ReadPieceRequest{
			PeerTaskMetadata: storage.PeerTaskMetadata{
				TaskID: taskID,
				PeerID: peerID,
			},
			PieceMetadata: storage.PieceMetadata{
				Num:   -1,
				Range: rg[0],
			},
		})
	if err != nil {
		log.Errorf("get task data failed: %s", err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
		return
	}
	defer closer.Close()

	// Add header "Content-Length" to avoid chunked body in http client.
	ctx.Header(headers.ContentLength, fmt.Sprintf("%d", rg[0].Length))

	// write header immediately, prevent client disconnecting after limiter.Wait() due to response header timeout
	ctx.Writer.WriteHeaderNow()
	ctx.Writer.Flush()

	if um.Limiter != nil {
		if err = um.Limiter.WaitN(ctx, int(rg[0].Length)); err != nil {
			log.Errorf("get limit failed: %s", err)
			ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
			return
		}
	}

	// If w is a socket, golang will use sendfile or splice syscall for zero copy feature
	// when start to transfer data, we could not call http.Error with header.
	if n, err := io.Copy(ctx.Writer, reader); err != nil {
		log.Errorf("transfer data failed: %s", err)
		return
	} else if n != rg[0].Length {
		log.Errorf("transferred data length not match request, request: %d, transferred: %d",
			rg[0].Length, n)
		return
	}
}
