/*
 *     Copyright 2022 The Dragonfly Authors
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

package objectstorage

import (
	"context"
	"errors"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-http-utils/headers"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	"d7y.io/dragonfly/v2/pkg/objectstorage"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
)

const (
	// defaultSignExpireTime is default expire of sign url.
	defaultSignExpireTime = 5 * time.Minute
)

// ObjectStorage is the interface used for object storage server.
type ObjectStorage interface {
	// Started object storage server.
	Serve(lis net.Listener) error

	// Stop object storage server.
	Stop() error
}

// objectStorage provides object storage function.
type objectStorage struct {
	*http.Server
	dynconfig       config.Dynconfig
	peerTaskManager peer.TaskManager
	storageManager  storage.Manager
	peerIDGenerator peer.IDGenerator
}

// New returns a new ObjectStorage instence.
func New(cfg *config.DaemonOption, dynconfig config.Dynconfig, peerTaskManager peer.TaskManager, storageManager storage.Manager) (ObjectStorage, error) {
	o := &objectStorage{
		dynconfig:       dynconfig,
		peerTaskManager: peerTaskManager,
		storageManager:  storageManager,
		peerIDGenerator: peer.NewPeerIDGenerator(cfg.Host.AdvertiseIP),
	}

	router := o.initRouter()
	o.Server = &http.Server{
		Handler: router,
	}

	return o, nil
}

// Started object storage server.
func (o *objectStorage) Serve(lis net.Listener) error {
	return o.Server.Serve(lis)
}

// Stop object storage server.
func (o *objectStorage) Stop() error {
	return o.Server.Shutdown(context.Background())
}

// Initialize router of gin.
func (o *objectStorage) initRouter() *gin.Engine {
	r := gin.Default()

	// Health Check.
	r.GET("/healthy", o.getHealth)

	// Buckets
	b := r.Group("/buckets")
	b.GET(":id/objects/*object_key", o.getObject)
	b.POST(":id/objects", o.createObject)

	return r
}

// getHealth uses to check server health.
func (o *objectStorage) getHealth(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, http.StatusText(http.StatusOK))
}

// getObject uses to download object data.
func (o *objectStorage) getObject(ctx *gin.Context) {
	var params GetObjectParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var (
		urlMeta       *base.UrlMeta
		artifactRange *clientutil.Range
		ranges        []clientutil.Range
		err           error
	)

	// Parse http range header.
	rangeHeader := ctx.GetHeader(headers.Range)
	if len(rangeHeader) > 0 {
		ranges, err = o.parseRangeHeader(rangeHeader)
		if err != nil {
			ctx.JSON(http.StatusRequestedRangeNotSatisfiable, gin.H{"errors": err.Error()})
			return
		}
		artifactRange = &ranges[0]

		// Range header in dragonfly is without "bytes=".
		urlMeta.Range = strings.TrimLeft(rangeHeader, "bytes=")
	}

	client, err := o.client()
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
		return
	}

	meta, err := client.GetObjectMetadata(ctx, params.ID, params.ObjectKey)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
		return
	}
	urlMeta.Digest = meta.Digest

	signURL, err := client.GetSignURL(ctx, params.ID, params.ObjectKey, objectstorage.MethodGet, defaultSignExpireTime)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
		return
	}

	reader, attr, err := o.peerTaskManager.StartStreamTask(ctx, &peer.StreamTaskRequest{
		URL:     signURL,
		URLMeta: urlMeta,
		Range:   artifactRange,
		PeerID:  o.peerIDGenerator.PeerID(),
	})
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
		return
	}
	defer reader.Close()

	var contentLength int64 = -1
	if l, ok := attr[headers.ContentLength]; ok {
		if i, err := strconv.ParseInt(l, 10, 64); err == nil {
			contentLength = i
		}
	}

	ctx.DataFromReader(http.StatusOK, contentLength, attr[headers.ContentType], reader, nil)
}

// createObject uses to upload object data.
func (o *objectStorage) createObject(ctx *gin.Context) {
	var params CreateObjectParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var form CreateObjectRequset
	if err := ctx.ShouldBind(&form); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}
}

// client uses to generate client of object storage.
func (o *objectStorage) client() (objectstorage.ObjectStorage, error) {
	config, err := o.dynconfig.GetObjectStorage()
	if err != nil {
		return nil, err
	}

	client, err := objectstorage.New(config.Name, config.Region, config.Endpoint, config.AccessKey, config.SecretKey)
	if err != nil {
		return nil, err
	}

	return client, nil
}

// parseRangeHeader uses to parse range http header for dragonfly.
func (o *objectStorage) parseRangeHeader(rangeHeader string) ([]clientutil.Range, error) {
	ranges, err := clientutil.ParseRange(rangeHeader, math.MaxInt)
	if err != nil {
		return nil, err
	}

	if len(ranges) > 1 {
		return nil, errors.New("multiple range is not supported")
	}

	if len(ranges) == 0 {
		return nil, errors.New("zero range is not supported")
	}

	return ranges, nil
}
