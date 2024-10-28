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
	mockPreheatJobReqBody = `
		{
			"type": "preheat",
			"user_id": 4,
			"bio": "bio"
		}`
	mockGetTaskJobReqBody = `
		{
			"type": "get_task",
			"user_id": 4,
			"bio": "bio",
			"args": {
				"task_id": "7575d21d69495905a4709bf4e10d0e5cffcf7fd1e76e93171e0ef6e0abcf07a8"
			}
		}`
	mockDeleteTaskJobReqBody = `
		{
			"type": "delete_task",
			"user_id": 4,
			"bio": "bio",
			"args": {
				"task_id": "04a29122b0c4d0affde2d577fb36bb956caa3da10e9130375623c24a5f865a49"
			}
		}`
	mockOtherJobReqBody = `
		{
			"type": "others",
			"user_id": 4,
			"bio": "bio"
		}`
	mockPreheatCreateJobRequest = types.CreatePreheatJobRequest{
		UserID: 4,
		Type:   "preheat",
		BIO:    "bio",
	}
	mockCreateGetTaskJobRequest = types.CreateGetTaskJobRequest{
		UserID: 4,
		Type:   "get_task",
		BIO:    "bio",
		Args:   types.GetTaskArgs{TaskID: "7575d21d69495905a4709bf4e10d0e5cffcf7fd1e76e93171e0ef6e0abcf07a8"},
	}
	mockCreateDeleteTaskJobRequest = types.CreateDeleteTaskJobRequest{
		UserID: 4,
		Type:   "delete_task",
		BIO:    "bio",
		Args: types.DeleteTaskArgs{
			TaskID: "04a29122b0c4d0affde2d577fb36bb956caa3da10e9130375623c24a5f865a49",
		},
	}
	mockUpdateJobRequest = types.UpdateJobRequest{
		UserID: 4,
		BIO:    "bio",
	}
	mockPreheatJobModel = &models.Job{
		BaseModel: mockBaseModel,
		UserID:    4,
		Type:      "preheat",
		BIO:       "bio",
		TaskID:    "dec6fe878785cea844dcecdf2ea25e19156822201016455733e47e9f0bfab563",
	}
	mockGetTaskJobModel = &models.Job{
		BaseModel: mockBaseModel,
		UserID:    4,
		Type:      "get_task",
		BIO:       "bio",
		TaskID:    "7575d21d69495905a4709bf4e10d0e5cffcf7fd1e76e93171e0ef6e0abcf07a8",
	}
	mockDeleteTaskJobModel = &models.Job{
		BaseModel: mockBaseModel,
		UserID:    4,
		Type:      "delete_task",
		BIO:       "bio",
		TaskID:    "04a29122b0c4d0affde2d577fb36bb956caa3da10e9130375623c24a5f865a49",
	}
)

func mockJobRouter(h *Handlers) *gin.Engine {
	r := gin.Default()
	oapiv1 := r.Group("/oapi/v1")
	ojob := oapiv1.Group("/jobs")
	ojob.POST("", h.CreateJob)
	ojob.DELETE(":id", h.DestroyJob)
	ojob.PATCH(":id", h.UpdateJob)
	ojob.GET(":id", h.GetJob)
	ojob.GET("", h.GetJobs)
	return r
}

func TestHandlers_CreateJob(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity by body",
			req:  httptest.NewRequest(http.MethodPost, "/oapi/v1/jobs", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "unprocessable entity by type",
			req:  httptest.NewRequest(http.MethodPost, "/oapi/v1/jobs", strings.NewReader(mockOtherJobReqBody)),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "create preheat job success",
			req:  httptest.NewRequest(http.MethodPost, "/oapi/v1/jobs", strings.NewReader(mockPreheatJobReqBody)),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.CreatePreheatJob(gomock.Any(), gomock.Eq(mockPreheatCreateJobRequest)).Return(mockPreheatJobModel, nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				job := models.Job{}
				err := json.Unmarshal(w.Body.Bytes(), &job)
				assert.NoError(err)
				assert.Equal(mockPreheatJobModel, &job)
			},
		},
		{
			name: "create get task job success",
			req:  httptest.NewRequest(http.MethodPost, "/oapi/v1/jobs", strings.NewReader(mockGetTaskJobReqBody)),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.CreateGetTaskJob(gomock.Any(), gomock.Eq(mockCreateGetTaskJobRequest)).Return(mockGetTaskJobModel, nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				job := models.Job{}
				err := json.Unmarshal(w.Body.Bytes(), &job)
				assert.NoError(err)
				assert.Equal(mockGetTaskJobModel, &job)
			},
		},
		{
			name: "create delete task job success",
			req:  httptest.NewRequest(http.MethodPost, "/oapi/v1/jobs", strings.NewReader(mockDeleteTaskJobReqBody)),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.CreateDeleteTaskJob(gomock.Any(), gomock.Eq(mockCreateDeleteTaskJobRequest)).Return(mockDeleteTaskJobModel, nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				job := models.Job{}
				err := json.Unmarshal(w.Body.Bytes(), &job)
				assert.NoError(err)
				assert.Equal(mockDeleteTaskJobModel, &job)
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
			mockRouter := mockJobRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_DestroyJob(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity",
			req:  httptest.NewRequest(http.MethodDelete, "/oapi/v1/jobs/test", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest(http.MethodDelete, "/oapi/v1/jobs/2", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.DestroyJob(gomock.Any(), gomock.Eq(uint(2))).Return(nil).Times(1)
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
			mockRouter := mockJobRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_UpdateJob(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity caused by uri",
			req:  httptest.NewRequest(http.MethodPatch, "/oapi/v1/jobs/test", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "unprocessable entity caused by body",
			req:  httptest.NewRequest(http.MethodPatch, "/oapi/v1/jobs/2", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest(http.MethodPatch, "/oapi/v1/jobs/2", strings.NewReader(mockPreheatJobReqBody)),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.UpdateJob(gomock.Any(), gomock.Eq(uint(2)), gomock.Eq(mockUpdateJobRequest)).Return(mockPreheatJobModel, nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				job := models.Job{}
				err := json.Unmarshal(w.Body.Bytes(), &job)
				assert.NoError(err)
				assert.Equal(mockPreheatJobModel, &job)
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
			mockRouter := mockJobRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_GetJob(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity",
			req:  httptest.NewRequest(http.MethodGet, "/oapi/v1/jobs/test", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest(http.MethodGet, "/oapi/v1/jobs/2", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.GetJob(gomock.Any(), gomock.Eq(uint(2))).Return(mockPreheatJobModel, nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				job := models.Job{}
				err := json.Unmarshal(w.Body.Bytes(), &job)
				assert.NoError(err)
				assert.Equal(mockPreheatJobModel, &job)
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
			mockRouter := mockJobRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_GetJobs(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity",
			req:  httptest.NewRequest(http.MethodGet, "/oapi/v1/jobs?page=-1", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest(http.MethodGet, "/oapi/v1/jobs?user_id=4", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.GetJobs(gomock.Any(), gomock.Eq(types.GetJobsQuery{
					UserID:  4,
					Page:    1,
					PerPage: 10,
				})).Return([]models.Job{*mockPreheatJobModel}, int64(1), nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				job := models.Job{}
				err := json.Unmarshal(w.Body.Bytes()[1:w.Body.Len()-1], &job)
				assert.NoError(err)
				assert.Equal(mockPreheatJobModel, &job)
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
			mockRouter := mockJobRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}
