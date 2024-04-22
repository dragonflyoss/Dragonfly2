/*
 *     Copyright 2024 The Dragonfly Authors
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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"d7y.io/dragonfly/v2/manager/service/mocks"
	"d7y.io/dragonfly/v2/manager/types"
)

var (
	mockClusterReqBody = `
		{
			"name": "name",
			"bio": "bio",
			"is_default": true,
			"peer_cluster_config": {
				"load_limit": 1
			},
			"scheduler_cluster_config": {
				"candidate_parent_limit": 1,
				"filter_parent_limit": 10
			},
			"seed_peer_cluster_config": {
				"load_limit": 1
			}
		}`
	mockCreateClusterResponse = &types.CreateClusterResponse{
		ID:                     2,
		Name:                   "foo",
		BIO:                    "bio",
		IsDefault:              true,
		SchedulerClusterID:     3,
		SeedPeerClusterID:      4,
		SchedulerClusterConfig: mockSchedulerClusterConfig,
		SeedPeerClusterConfig:  mockSeedPeerClusterConfig,
		PeerClusterConfig:      mockPeerClusterConfig,
	}
	mockUpdateClusterResponse = &types.UpdateClusterResponse{
		ID:                     2,
		Name:                   "foo",
		BIO:                    "bio",
		IsDefault:              true,
		SchedulerClusterID:     3,
		SeedPeerClusterID:      4,
		SchedulerClusterConfig: mockSchedulerClusterConfig,
		SeedPeerClusterConfig:  mockSeedPeerClusterConfig,
		PeerClusterConfig:      mockPeerClusterConfig,
	}
	mockGetClusterResponse = &types.GetClusterResponse{
		ID:                     2,
		Name:                   "foo",
		BIO:                    "bio",
		IsDefault:              true,
		SchedulerClusterID:     3,
		SeedPeerClusterID:      4,
		SchedulerClusterConfig: mockSchedulerClusterConfig,
		SeedPeerClusterConfig:  mockSeedPeerClusterConfig,
		PeerClusterConfig:      mockPeerClusterConfig,
	}
	mockSchedulerClusterConfig = &types.SchedulerClusterConfig{
		CandidateParentLimit: 1,
		FilterParentLimit:    10,
	}
	mockSeedPeerClusterConfig = &types.SeedPeerClusterConfig{
		LoadLimit: 1,
	}
	mockPeerClusterConfig = &types.SchedulerClusterClientConfig{
		LoadLimit: 1,
	}
)

func mockClusterRouter(h *Handlers) *gin.Engine {
	r := gin.Default()
	oapiv1 := r.Group("/oapi/v1")
	oc := oapiv1.Group("/clusters")
	oc.POST("", h.CreateCluster)
	oc.DELETE(":id", h.DestroyCluster)
	oc.PATCH(":id", h.UpdateCluster)
	oc.GET(":id", h.GetCluster)
	oc.GET("", h.GetClusters)
	return r
}

func TestHandlers_CreateCluster(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity",
			req:  httptest.NewRequest(http.MethodPost, "/oapi/v1/clusters", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest(http.MethodPost, "/oapi/v1/clusters", strings.NewReader(mockClusterReqBody)),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.CreateCluster(gomock.Any(), gomock.Any()).Return(mockCreateClusterResponse, nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				createClusterResponse := types.CreateClusterResponse{}
				err := json.Unmarshal(w.Body.Bytes(), &createClusterResponse)
				assert.NoError(err)
				assert.Equal(mockCreateClusterResponse, &createClusterResponse)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			svc := mocks.NewMockService(ctl)
			w := httptest.NewRecorder()
			h := New(svc)
			mockRouter := mockClusterRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_DestroyCluster(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity",
			req:  httptest.NewRequest(http.MethodDelete, "/oapi/v1/clusters/test", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest(http.MethodDelete, "/oapi/v1/clusters/2", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.DestroyCluster(gomock.Any(), gomock.Eq(uint(2))).Return(nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			svc := mocks.NewMockService(ctl)
			w := httptest.NewRecorder()
			h := New(svc)
			mockRouter := mockClusterRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_UpdateCluster(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity caused by uri",
			req:  httptest.NewRequest(http.MethodPatch, "/oapi/v1/clusters/test", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "unprocessable entity caused by body",
			req:  httptest.NewRequest(http.MethodPatch, "/oapi/v1/clusters/2", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest(http.MethodPatch, "/oapi/v1/clusters/2", strings.NewReader(mockClusterReqBody)),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.UpdateCluster(gomock.Any(), gomock.Eq(uint(2)), gomock.Any()).Return(mockUpdateClusterResponse, nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				updateClusterResponse := types.UpdateClusterResponse{}
				err := json.Unmarshal(w.Body.Bytes(), &updateClusterResponse)
				assert.NoError(err)
				assert.Equal(mockUpdateClusterResponse, &updateClusterResponse)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			svc := mocks.NewMockService(ctl)
			w := httptest.NewRecorder()
			h := New(svc)
			mockRouter := mockClusterRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_GetCluster(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity",
			req:  httptest.NewRequest(http.MethodGet, "/oapi/v1/clusters/test", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest(http.MethodGet, "/oapi/v1/clusters/2", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.GetCluster(gomock.Any(), gomock.Eq(uint(2))).Return(mockGetClusterResponse, nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				getClusterResponse := types.GetClusterResponse{}
				err := json.Unmarshal(w.Body.Bytes(), &getClusterResponse)
				assert.NoError(err)
				assert.Equal(mockGetClusterResponse, &getClusterResponse)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			svc := mocks.NewMockService(ctl)
			w := httptest.NewRecorder()
			h := New(svc)
			mockRouter := mockClusterRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_GetClusters(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity",
			req:  httptest.NewRequest(http.MethodGet, "/oapi/v1/clusters?page=-1", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest(http.MethodGet, "/oapi/v1/clusters", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.GetClusters(gomock.Any(), gomock.Eq(types.GetClustersQuery{
					Name:    "",
					Page:    1,
					PerPage: 10,
				})).Return([]types.GetClusterResponse{*mockGetClusterResponse}, int64(1), nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				getClusterResponse := types.GetClusterResponse{}
				err := json.Unmarshal(w.Body.Bytes()[1:w.Body.Len()-1], &getClusterResponse)
				assert.NoError(err)
				assert.Equal(mockGetClusterResponse, &getClusterResponse)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			svc := mocks.NewMockService(ctl)
			w := httptest.NewRecorder()
			h := New(svc)
			mockRouter := mockClusterRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}
