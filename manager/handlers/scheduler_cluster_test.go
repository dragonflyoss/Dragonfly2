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

	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/service/mocks"
	"d7y.io/dragonfly/v2/manager/types"
)

var (
	mockSchedulerClusterReqBody = `
		{
		   "bio": "bio",
		   "client_config": {
			  "load_limit": 1
		   },
		   "config": {
			  "candidate_parent_limit": 1,
			  "filter_parent_limit": 10
		   },
		   "is_default": false,
		   "name": "foo",
		   "seed_peer_cluster_id": 2
		}`
	mockSchedulerClusterModel = &models.SchedulerCluster{
		BaseModel:    mockBaseModel,
		Name:         "foo",
		BIO:          "bio",
		IsDefault:    false,
		Config:       models.JSONMap{"CandidateParentLimit": 1, "FilterParentLimit": 10},
		ClientConfig: models.JSONMap{"LoadLimit": 1},
	}
	mockUnmarshalSchedulerClusterModel = &models.SchedulerCluster{
		BaseModel:    mockBaseModel,
		Name:         "foo",
		BIO:          "bio",
		IsDefault:    false,
		Config:       models.JSONMap{"CandidateParentLimit": float64(1), "FilterParentLimit": float64(10)},
		ClientConfig: models.JSONMap{"LoadLimit": float64(1)},
	}
)

func mockSchedulerClusterRouter(h *Handlers) *gin.Engine {
	r := gin.Default()
	apiv1 := r.Group("/api/v1")
	sc := apiv1.Group("/scheduler-clusters")
	sc.POST("", h.CreateSchedulerCluster)
	sc.DELETE(":id", h.DestroySchedulerCluster)
	sc.PATCH(":id", h.UpdateSchedulerCluster)
	sc.GET(":id", h.GetSchedulerCluster)
	sc.GET("", h.GetSchedulerClusters)
	sc.PUT(":id/schedulers/:scheduler_id", h.AddSchedulerToSchedulerCluster)
	return r
}

func TestHandlers_CreateSchedulerCluster(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity",
			req:  httptest.NewRequest(http.MethodPost, "/api/v1/scheduler-clusters", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest(http.MethodPost, "/api/v1/scheduler-clusters", strings.NewReader(mockSchedulerClusterReqBody)),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.CreateSchedulerCluster(gomock.Any(), gomock.Any()).Return(mockSchedulerClusterModel, nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				schedulerCluster := models.SchedulerCluster{}
				err := json.Unmarshal(w.Body.Bytes(), &schedulerCluster)
				assert.NoError(err)
				assert.Equal(mockUnmarshalSchedulerClusterModel, &schedulerCluster)
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
			mockRouter := mockSchedulerClusterRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_DestroySchedulerCluster(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity",
			req:  httptest.NewRequest(http.MethodDelete, "/api/v1/scheduler-clusters/test", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest(http.MethodDelete, "/api/v1/scheduler-clusters/2", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.DestroySchedulerCluster(gomock.Any(), gomock.Eq(uint(2))).Return(nil).Times(1)
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
			mockRouter := mockSchedulerClusterRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_UpdateSchedulerCluster(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity caused by uri",
			req:  httptest.NewRequest(http.MethodPatch, "/api/v1/scheduler-clusters/test", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "unprocessable entity caused by body",
			req:  httptest.NewRequest(http.MethodPatch, "/api/v1/scheduler-clusters/2", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest(http.MethodPatch, "/api/v1/scheduler-clusters/2", strings.NewReader(mockSchedulerClusterReqBody)),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.UpdateSchedulerCluster(gomock.Any(), gomock.Eq(uint(2)), gomock.Any()).Return(mockSchedulerClusterModel, nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				schedulerCluster := models.SchedulerCluster{}
				err := json.Unmarshal(w.Body.Bytes(), &schedulerCluster)
				assert.NoError(err)
				assert.Equal(mockUnmarshalSchedulerClusterModel, &schedulerCluster)
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
			mockRouter := mockSchedulerClusterRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_GetSchedulerCluster(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity",
			req:  httptest.NewRequest(http.MethodGet, "/api/v1/scheduler-clusters/test", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest(http.MethodGet, "/api/v1/scheduler-clusters/2", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.GetSchedulerCluster(gomock.Any(), gomock.Eq(uint(2))).Return(mockSchedulerClusterModel, nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				schedulerCluster := models.SchedulerCluster{}
				err := json.Unmarshal(w.Body.Bytes(), &schedulerCluster)
				assert.NoError(err)
				assert.Equal(mockUnmarshalSchedulerClusterModel, &schedulerCluster)
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
			mockRouter := mockSchedulerClusterRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_GetSchedulerClusters(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity",
			req:  httptest.NewRequest(http.MethodGet, "/api/v1/scheduler-clusters?page=-1", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest(http.MethodGet, "/api/v1/scheduler-clusters", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.GetSchedulerClusters(gomock.Any(), gomock.Eq(types.GetSchedulerClustersQuery{
					Page:    1,
					PerPage: 10,
				})).Return([]models.SchedulerCluster{*mockSchedulerClusterModel}, int64(1), nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				schedulerCluster := models.SchedulerCluster{}
				err := json.Unmarshal(w.Body.Bytes()[1:w.Body.Len()-1], &schedulerCluster)
				assert.NoError(err)
				assert.Equal(mockUnmarshalSchedulerClusterModel, &schedulerCluster)
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
			mockRouter := mockSchedulerClusterRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_AddSchedulerToSchedulerCluster(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity",
			req:  httptest.NewRequest(http.MethodPut, "/api/v1/scheduler-clusters/4/schedulers/test", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest(http.MethodPut, "/api/v1/scheduler-clusters/4/schedulers/2", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.AddSchedulerToSchedulerCluster(gomock.Any(), gomock.Eq(uint(4)), gomock.Eq(uint(2))).Return(nil).Times(1)
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
			mockRouter := mockSchedulerClusterRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}
