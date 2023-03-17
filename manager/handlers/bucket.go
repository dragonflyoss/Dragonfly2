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

package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	// nolint
	_ "d7y.io/dragonfly/v2/manager/models"
	// nolint
	"d7y.io/dragonfly/v2/manager/types"
	// nolint
	_ "d7y.io/dragonfly/v2/pkg/objectstorage"
)

// @Summary Create Bucket
// @Description Create by json bucket
// @Tags Bucket
// @Accept json
// @Produce json
// @Param Bucket body types.CreateBucketRequest true "Bucket"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /buckets [post]
func (h *Handlers) CreateBucket(ctx *gin.Context) {
	var json types.CreateBucketRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.CreateBucket(ctx.Request.Context(), json); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Destroy Bucket
// @Description Destroy by id
// @Tags Bucket
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /buckets/{id} [delete]
func (h *Handlers) DestroyBucket(ctx *gin.Context) {
	var params types.BucketParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DestroyBucket(ctx.Request.Context(), params.ID); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Get Bucket
// @Description Get Bucket by id
// @Tags Bucket
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} objectstorage.BucketMetadata
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /buckets/{id} [get]
func (h *Handlers) GetBucket(ctx *gin.Context) {
	var params types.BucketParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	bucket, err := h.service.GetBucket(ctx.Request.Context(), params.ID)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, bucket)
}

// @Summary Get Buckets
// @Description Get Buckets
// @Tags Bucket
// @Accept json
// @Produce json
// @Success 200 {object} []objectstorage.BucketMetadata
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /buckets [get]
func (h *Handlers) GetBuckets(ctx *gin.Context) {
	buckets, err := h.service.GetBuckets(ctx.Request.Context())
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, buckets)
}
