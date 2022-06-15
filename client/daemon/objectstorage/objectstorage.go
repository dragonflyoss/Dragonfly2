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
	"net"
	"net/http"

	"github.com/gin-gonic/gin"
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
}

// New returns a new ObjectStorage instence.
func New() ObjectStorage {
	return &objectStorage{}
}

// Started object storage server.
func (o *objectStorage) Serve(lis net.Listener) error {
	return nil
}

// Stop object storage server.
func (o *objectStorage) Stop() error {
	return nil
}

// Initialize router of gin.
func (o *objectStorage) initRouter() *gin.Engine {
	r := gin.Default()

	// Health Check.
	r.GET("/healthy", o.getHealth)

	// Buckets
	b := r.Group("/buckets")
	b.GET(":id/objects/*object_key", o.getObject)
	b.POST(":id/objects/*object_key", o.createObject)

	return r
}

// getHealth uses to check server health.
func (o *objectStorage) getHealth(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, http.StatusText(http.StatusOK))
}

// getObject uses to download object data.
func (o *objectStorage) getObject(ctx *gin.Context) {
}

// createObject uses to upload object data.
func (o *objectStorage) createObject(ctx *gin.Context) {
}
