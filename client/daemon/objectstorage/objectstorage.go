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

//go:generate mockgen -destination mocks/objectstorage_mock.go -source objectstorage.go -package mocks

package objectstorage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-http-utils/headers"
	ginprometheus "github.com/mcuadros/go-gin-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"

	commonv1 "d7y.io/api/pkg/apis/common/v1"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/pkg/idgen"
	nethttp "d7y.io/dragonfly/v2/pkg/net/http"
	"d7y.io/dragonfly/v2/pkg/objectstorage"
	pkgstrings "d7y.io/dragonfly/v2/pkg/strings"
)

const (
	// AsyncWriteBack writes the object asynchronously to the backend.
	AsyncWriteBack = iota

	// WriteBack writes the object synchronously to the backend.
	WriteBack

	// Ephemeral only writes the object to the dfdaemon.
	// It is only provided for creating temporary objects between peers,
	// and users are not allowed to use this mode.
	Ephemeral
)

const (
	PrometheusSubsystemName = "dragonfly_dfdaemon_object_stroage"
	OtelServiceName         = "dragonfly-dfdaemon-object-storage"
)

const (
	RouterGroupBuckets = "/buckets"
)

var GinLogFileName = "gin-object-stroage.log"

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
	config          *config.DaemonOption
	dynconfig       config.Dynconfig
	peerTaskManager peer.TaskManager
	storageManager  storage.Manager
	peerIDGenerator peer.IDGenerator
}

// New returns a new ObjectStorage instence.
func New(cfg *config.DaemonOption, dynconfig config.Dynconfig, peerTaskManager peer.TaskManager, storageManager storage.Manager, logDir string) (ObjectStorage, error) {
	o := &objectStorage{
		config:          cfg,
		dynconfig:       dynconfig,
		peerTaskManager: peerTaskManager,
		storageManager:  storageManager,
		peerIDGenerator: peer.NewPeerIDGenerator(cfg.Host.AdvertiseIP.String()),
	}

	router := o.initRouter(cfg, logDir)
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
func (o *objectStorage) initRouter(cfg *config.DaemonOption, logDir string) *gin.Engine {
	// Set mode
	if !cfg.Verbose {
		gin.SetMode(gin.ReleaseMode)
	}

	// Logging to a file
	if !cfg.Console {
		gin.DisableConsoleColor()
		logDir := filepath.Join(logDir, "daemon")
		f, _ := os.Create(filepath.Join(logDir, GinLogFileName))
		gin.DefaultWriter = io.MultiWriter(f)
	}

	r := gin.New()

	// Middleware
	r.Use(gin.Logger())
	r.Use(gin.Recovery())

	// Prometheus metrics
	p := ginprometheus.NewPrometheus(PrometheusSubsystemName)
	// Prometheus metrics need to reduce label,
	// refer to https://prometheus.io/docs/practices/instrumentation/#do-not-overuse-labels.
	p.ReqCntURLLabelMappingFn = func(c *gin.Context) string {
		if strings.HasPrefix(c.Request.URL.Path, RouterGroupBuckets) {
			return RouterGroupBuckets
		}

		return c.Request.URL.Path
	}
	p.Use(r)

	// Opentelemetry
	if cfg.Options.Telemetry.Jaeger != "" {
		r.Use(otelgin.Middleware(OtelServiceName))
	}

	// Health Check.
	r.GET("/healthy", o.getHealth)

	// Buckets
	b := r.Group(RouterGroupBuckets)
	b.HEAD(":id/objects/*object_key", o.headObject)
	b.GET(":id/objects/*object_key", o.getObject)
	b.DELETE(":id/objects/*object_key", o.destroyObject)
	b.PUT(":id/objects/*object_key", o.putObject)

	return r
}

// getHealth uses to check server health.
func (o *objectStorage) getHealth(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, http.StatusText(http.StatusOK))
}

// headObject uses to head object.
func (o *objectStorage) headObject(ctx *gin.Context) {
	var params ObjectParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var (
		bucketName = params.ID
		objectKey  = strings.TrimPrefix(params.ObjectKey, string(os.PathSeparator))
	)

	client, err := o.client()
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
		return
	}

	meta, isExist, err := client.GetObjectMetadata(ctx, bucketName, objectKey)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
		return
	}

	if !isExist {
		ctx.JSON(http.StatusNotFound, gin.H{"errors": http.StatusText(http.StatusNotFound)})
		return
	}

	ctx.Header(headers.ContentDisposition, meta.ContentDisposition)
	ctx.Header(headers.ContentEncoding, meta.ContentEncoding)
	ctx.Header(headers.ContentLanguage, meta.ContentLanguage)
	ctx.Header(headers.ContentLength, fmt.Sprint(meta.ContentLength))
	ctx.Header(headers.ContentType, meta.ContentType)
	ctx.Header(headers.ETag, meta.ETag)
	ctx.Header(config.HeaderDragonflyObjectMetaDigest, meta.Digest)

	ctx.Status(http.StatusOK)
	return
}

// getObject uses to download object data.
func (o *objectStorage) getObject(ctx *gin.Context) {
	var params ObjectParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var query GetObjectQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var (
		bucketName = params.ID
		objectKey  = strings.TrimPrefix(params.ObjectKey, string(os.PathSeparator))
		filter     = query.Filter
		rg         *nethttp.Range
		err        error
	)

	// Initialize filter field.
	urlMeta := &commonv1.UrlMeta{Filter: o.config.ObjectStorage.Filter}
	if filter != "" {
		urlMeta.Filter = filter
	}

	client, err := o.client()
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
		return
	}

	meta, isExist, err := client.GetObjectMetadata(ctx, bucketName, objectKey)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
		return
	}

	if !isExist {
		ctx.JSON(http.StatusNotFound, gin.H{"errors": http.StatusText(http.StatusNotFound)})
		return
	}

	urlMeta.Digest = meta.Digest

	// Parse http range header.
	rangeHeader := ctx.GetHeader(headers.Range)
	if len(rangeHeader) > 0 {
		rangeValue, err := nethttp.ParseOneRange(rangeHeader, math.MaxInt64)
		if err != nil {
			ctx.JSON(http.StatusRequestedRangeNotSatisfiable, gin.H{"errors": err.Error()})
			return
		}
		rg = &rangeValue

		// Range header in dragonfly is without "bytes=".
		urlMeta.Range = strings.TrimLeft(rangeHeader, "bytes=")

		// When the request has a range header,
		// there is no need to calculate md5, set this value to empty.
		urlMeta.Digest = ""
	}

	signURL, err := client.GetSignURL(ctx, bucketName, objectKey, objectstorage.MethodGet, defaultSignExpireTime)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
		return
	}

	taskID := idgen.TaskIDV1(signURL, urlMeta)
	log := logger.WithTaskID(taskID)
	log.Infof("get object %s meta: %s %#v", objectKey, signURL, urlMeta)

	reader, attr, err := o.peerTaskManager.StartStreamTask(ctx, &peer.StreamTaskRequest{
		URL:     signURL,
		URLMeta: urlMeta,
		Range:   rg,
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

	log.Infof("object content length is %d and content type is %s", contentLength, attr[headers.ContentType])
	ctx.DataFromReader(http.StatusOK, contentLength, attr[headers.ContentType], reader, nil)
}

// destroyObject uses to delete object data.
func (o *objectStorage) destroyObject(ctx *gin.Context) {
	var params ObjectParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	client, err := o.client()
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
		return
	}

	var (
		bucketName = params.ID
		objectKey  = strings.TrimPrefix(params.ObjectKey, string(os.PathSeparator))
	)

	logger.Infof("destroy object %s in bucket %s", objectKey, bucketName)
	if err := client.DeleteObject(ctx, bucketName, objectKey); err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
		return
	}

	ctx.Status(http.StatusOK)
	return
}

// putObject uses to upload object data.
func (o *objectStorage) putObject(ctx *gin.Context) {
	var params ObjectParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var form PutObjectRequset
	if err := ctx.ShouldBind(&form); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var (
		bucketName  = params.ID
		objectKey   = strings.TrimPrefix(params.ObjectKey, string(os.PathSeparator))
		mode        = form.Mode
		filter      = form.Filter
		maxReplicas = form.MaxReplicas
		fileHeader  = form.File
	)

	client, err := o.client()
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
		return
	}

	signURL, err := client.GetSignURL(ctx, bucketName, objectKey, objectstorage.MethodGet, defaultSignExpireTime)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
		return
	}

	// Initialize url meta.
	urlMeta := &commonv1.UrlMeta{Filter: o.config.ObjectStorage.Filter}
	dgst := o.md5FromFileHeader(fileHeader)
	urlMeta.Digest = dgst.String()
	if filter != "" {
		urlMeta.Filter = filter
	}

	// Initialize max replicas.
	if maxReplicas == 0 {
		maxReplicas = o.config.ObjectStorage.MaxReplicas
	}

	// Initialize task id and peer id.
	taskID := idgen.TaskIDV1(signURL, urlMeta)
	peerID := o.peerIDGenerator.PeerID()

	log := logger.WithTaskAndPeerID(taskID, peerID)
	log.Infof("upload object %s meta: %s %#v", objectKey, signURL, urlMeta)

	// Import object to local storage.
	log.Infof("import object %s to local storage", objectKey)
	if err := o.importObjectToLocalStorage(ctx, taskID, peerID, fileHeader); err != nil {
		log.Error(err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
		return
	}

	// Announce peer information to scheduler.
	log.Info("announce peer to scheduler")
	if err := o.peerTaskManager.AnnouncePeerTask(ctx, storage.PeerTaskMetadata{
		TaskID: taskID,
		PeerID: peerID,
	}, signURL, commonv1.TaskType_DfStore, urlMeta); err != nil {
		log.Error(err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
		return
	}

	// Handle task for backend.
	switch mode {
	case Ephemeral:
		ctx.Status(http.StatusOK)
		return
	case WriteBack:
		// Import object to seed peer.
		go func() {
			if err := o.importObjectToSeedPeers(context.Background(), bucketName, objectKey, urlMeta.Filter, Ephemeral, fileHeader, maxReplicas, log); err != nil {
				log.Errorf("import object %s to seed peers failed: %s", objectKey, err)
			}
		}()

		// Import object to object storage.
		log.Infof("import object %s to bucket %s", objectKey, bucketName)
		if err := o.importObjectToBackend(ctx, bucketName, objectKey, dgst, fileHeader, client); err != nil {
			log.Error(err)
			ctx.JSON(http.StatusInternalServerError, gin.H{"errors": err.Error()})
			return
		}

		ctx.Status(http.StatusOK)
		return
	case AsyncWriteBack:
		// Import object to seed peer.
		go func() {
			if err := o.importObjectToSeedPeers(context.Background(), bucketName, objectKey, urlMeta.Filter, Ephemeral, fileHeader, maxReplicas, log); err != nil {
				log.Errorf("import object %s to seed peers failed: %s", objectKey, err)
			}
		}()

		// Import object to object storage.
		go func() {
			log.Infof("import object %s to bucket %s", objectKey, bucketName)
			if err := o.importObjectToBackend(context.Background(), bucketName, objectKey, dgst, fileHeader, client); err != nil {
				log.Errorf("import object %s to bucket %s failed: %s", objectKey, bucketName, err.Error())
				return
			}
		}()

		ctx.Status(http.StatusOK)
		return
	}

	ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": fmt.Sprintf("unknow mode %d", mode)})
	return
}

// getAvailableSeedPeer uses to calculate md5 with file header.
func (o *objectStorage) md5FromFileHeader(fileHeader *multipart.FileHeader) *digest.Digest {
	f, err := fileHeader.Open()
	if err != nil {
		return nil
	}
	defer f.Close()

	return digest.New(digest.AlgorithmMD5, digest.MD5FromReader(f))
}

// importObjectToBackend uses to import object to backend.
func (o *objectStorage) importObjectToBackend(ctx context.Context, bucketName, objectKey string, dgst *digest.Digest, fileHeader *multipart.FileHeader, client objectstorage.ObjectStorage) error {
	f, err := fileHeader.Open()
	if err != nil {
		return err
	}
	defer f.Close()

	if err := client.PutObject(ctx, bucketName, objectKey, dgst.String(), f); err != nil {
		return err
	}
	return nil
}

// importObjectToSeedPeers uses to import object to local storage.
func (o *objectStorage) importObjectToLocalStorage(ctx context.Context, taskID, peerID string, fileHeader *multipart.FileHeader) error {
	f, err := fileHeader.Open()
	if err != nil {
		return nil
	}
	defer f.Close()

	meta := storage.PeerTaskMetadata{
		TaskID: taskID,
		PeerID: peerID,
	}

	// Register task.
	tsd, err := o.storageManager.RegisterTask(ctx, &storage.RegisterTaskRequest{
		PeerTaskMetadata: meta,
	})
	if err != nil {
		return err
	}

	// Import task data to dfdaemon.
	if err := o.peerTaskManager.GetPieceManager().Import(ctx, meta, tsd, fileHeader.Size, f); err != nil {
		return err
	}

	return nil
}

// importObjectToSeedPeers uses to import object to available seed peers.
func (o *objectStorage) importObjectToSeedPeers(ctx context.Context, bucketName, objectKey, filter string, mode int, fileHeader *multipart.FileHeader, maxReplicas int, log *logger.SugaredLoggerOnWith) error {
	schedulers, err := o.dynconfig.GetSchedulers()
	if err != nil {
		return err
	}

	var seedPeerHosts []string
	for _, scheduler := range schedulers {
		for _, seedPeer := range scheduler.SeedPeers {
			if o.config.Host.AdvertiseIP.String() != seedPeer.Ip && seedPeer.ObjectStoragePort > 0 {
				seedPeerHosts = append(seedPeerHosts, fmt.Sprintf("%s:%d", seedPeer.Ip, seedPeer.ObjectStoragePort))
			}
		}
	}
	seedPeerHosts = pkgstrings.Unique(seedPeerHosts)

	var replicas int
	for _, seedPeerHost := range seedPeerHosts {
		log.Infof("import object %s to seed peer %s", objectKey, seedPeerHost)
		if err := o.importObjectToSeedPeer(ctx, seedPeerHost, bucketName, objectKey, filter, mode, fileHeader); err != nil {
			log.Errorf("import object %s to seed peer %s failed: %s", objectKey, seedPeerHost, err)
			continue
		}

		replicas++
		if replicas >= maxReplicas {
			break
		}
	}

	log.Infof("import %d object %s to seed peers", replicas, objectKey)
	return nil
}

// importObjectToSeedPeer uses to import object to seed peer.
func (o *objectStorage) importObjectToSeedPeer(ctx context.Context, seedPeerHost, bucketName, objectKey, filter string, mode int, fileHeader *multipart.FileHeader) error {
	f, err := fileHeader.Open()
	if err != nil {
		return err
	}
	defer f.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	if err := writer.WriteField("mode", fmt.Sprint(mode)); err != nil {
		return err
	}

	if filter != "" {
		if err := writer.WriteField("filter", filter); err != nil {
			return err
		}
	}

	part, err := writer.CreateFormFile("file", fileHeader.Filename)
	if err != nil {
		return err
	}

	if _, err := io.Copy(part, f); err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return err
	}

	u := url.URL{
		Scheme: "http",
		Host:   seedPeerHost,
		Path:   filepath.Join("buckets", bucketName, "objects", objectKey),
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), body)
	if err != nil {
		return err
	}
	req.Header.Add(headers.ContentType, writer.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("bad response status %s", resp.Status)
	}

	return nil
}

// client uses to generate client of object storage.
func (o *objectStorage) client() (objectstorage.ObjectStorage, error) {
	config, err := o.dynconfig.GetObjectStorage()
	if err != nil {
		return nil, err
	}

	client, err := objectstorage.New(config.Name, config.Region, config.Endpoint,
		config.AccessKey, config.SecretKey, objectstorage.WithS3ForcePathStyle(config.S3ForcePathStyle))
	if err != nil {
		return nil, err
	}

	return client, nil
}
