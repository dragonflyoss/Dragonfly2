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
	"net"
	"net/http"

	"github.com/gin-gonic/gin"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/peer"
	"d7y.io/dragonfly/v2/client/daemon/storage"
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
	dynconfig      config.Dynconfig
	peeTaskManager peer.TaskManager
	storageManager storage.Manager
}

// New returns a new ObjectStorage instence.
func New(dynconfig config.Dynconfig, peerTaskManager peer.TaskManager, storageManager storage.Manager) ObjectStorage {
	o := &objectStorage{
		dynconfig:      dynconfig,
		peeTaskManager: peerTaskManager,
		storageManager: storageManager,
	}

	router := o.initRouter()
	o.Server = &http.Server{
		Handler: router,
	}

	return o
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
