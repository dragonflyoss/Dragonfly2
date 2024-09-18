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
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"gorm.io/plugin/soft_delete"

	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/service/mocks"
	"d7y.io/dragonfly/v2/manager/types"
)

var (
	mockApplicationReqBody = `
		{
		   "bio": "bio",
		   "name": "foo",
		   "priority": {
			  "urls": [
				 {
					"regex": "regex",
					"value": 20
				 }
			  ],
			  "value": 20
		   },
		   "url": "http://example.com/foo",
		   "user_id": 4
		}`
	mockPriorityValue            = 20
	mockCreateApplicationRequest = types.CreateApplicationRequest{
		Name:   "foo",
		URL:    "http://example.com/foo",
		BIO:    "bio",
		UserID: 4,
		Priority: &types.PriorityConfig{
			Value: &mockPriorityValue,
			URLs: []types.URLPriorityConfig{
				{
					Regex: "regex",
					Value: 20,
				},
			},
		},
	}
	mockUpdateApplicationRequest = types.UpdateApplicationRequest{
		Name:   "foo",
		URL:    "http://example.com/foo",
		BIO:    "bio",
		UserID: 4,
		Priority: &types.PriorityConfig{
			Value: &mockPriorityValue,
			URLs: []types.URLPriorityConfig{
				{
					Regex: "regex",
					Value: 20,
				},
			},
		},
	}
	mockBaseModel = models.BaseModel{
		ID:        2,
		CreatedAt: time.Date(2024, 4, 17, 18, 0, 21, 580470900, time.UTC),
		UpdatedAt: time.Date(2024, 4, 17, 18, 0, 21, 580470900, time.UTC),
		IsDel:     soft_delete.DeletedAt(soft_delete.FlagActived),
	}
	mockApplicationModel = &models.Application{
		BaseModel: mockBaseModel,
		Name:      "foo",
		URL:       "http://example.com/foo",
		BIO:       "bio",
		UserID:    4,
		Priority:  models.JSONMap{"value": 20, "urls": []any{map[string]any{"regex": "regex", "value": 20}}},
	}
	mockUnmarshalApplicationModel = models.Application{
		BaseModel: mockBaseModel,
		Name:      "foo",
		URL:       "http://example.com/foo",
		BIO:       "bio",
		UserID:    4,
		// when w.Body.Bytes() is unmarshal to models.Application, the value of Priority will be float64
		Priority: models.JSONMap{"value": float64(20), "urls": []any{map[string]any{"regex": "regex", "value": float64(20)}}},
	}
)

func mockApplicationRouter(h *Handlers) *gin.Engine {
	r := gin.Default()
	apiv1 := r.Group("/api/v1")
	cs := apiv1.Group("/applications")
	cs.POST("", h.CreateApplication)
	cs.DELETE(":id", h.DestroyApplication)
	cs.PATCH(":id", h.UpdateApplication)
	cs.GET(":id", h.GetApplication)
	cs.GET("", h.GetApplications)
	return r
}

func TestHandlers_CreateApplication(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity",
			req:  httptest.NewRequest(http.MethodPost, "/api/v1/applications", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest(http.MethodPost, "/api/v1/applications", strings.NewReader(mockApplicationReqBody)),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.CreateApplication(gomock.Any(), gomock.Eq(mockCreateApplicationRequest)).Return(mockApplicationModel, nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				application := models.Application{}
				err := json.Unmarshal(w.Body.Bytes(), &application)
				assert.NoError(err)
				assert.Equal(mockUnmarshalApplicationModel, application)
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
			mockRouter := mockApplicationRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_DestroyApplication(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity",
			req:  httptest.NewRequest(http.MethodDelete, "/api/v1/applications/test", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest(http.MethodDelete, "/api/v1/applications/2", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.DestroyApplication(gomock.Any(), gomock.Eq(uint(2))).Return(nil).Times(1)
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
			mockRouter := mockApplicationRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_UpdateApplication(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity caused by uri",
			req:  httptest.NewRequest(http.MethodPatch, "/api/v1/applications/test", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "unprocessable entity caused by body",
			req:  httptest.NewRequest(http.MethodPatch, "/api/v1/applications/2", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest(http.MethodPatch, "/api/v1/applications/2", strings.NewReader(mockApplicationReqBody)),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.UpdateApplication(gomock.Any(), gomock.Eq(uint(2)), gomock.Eq(mockUpdateApplicationRequest)).Return(mockApplicationModel, nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				application := models.Application{}
				err := json.Unmarshal(w.Body.Bytes(), &application)
				assert.NoError(err)
				assert.Equal(mockUnmarshalApplicationModel, application)
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
			mockRouter := mockApplicationRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_GetApplication(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity",
			req:  httptest.NewRequest(http.MethodGet, "/api/v1/applications/test", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest(http.MethodGet, "/api/v1/applications/2", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.GetApplication(gomock.Any(), gomock.Eq(uint(2))).Return(mockApplicationModel, nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				application := models.Application{}
				err := json.Unmarshal(w.Body.Bytes(), &application)
				assert.NoError(err)
				assert.Equal(mockUnmarshalApplicationModel, application)
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
			mockRouter := mockApplicationRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_GetApplications(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity",
			req:  httptest.NewRequest(http.MethodGet, "/api/v1/applications?page=-1", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest(http.MethodGet, "/api/v1/applications", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.GetApplications(gomock.Any(), gomock.Eq(types.GetApplicationsQuery{
					Name:    "",
					Page:    1,
					PerPage: 10,
				})).Return([]models.Application{*mockApplicationModel}, int64(1), nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				application := models.Application{}
				// Remove the first and last character "[]" of the response body,
				// because the response body is a list of models.Application.
				err := json.Unmarshal(w.Body.Bytes()[1:w.Body.Len()-1], &application)
				assert.NoError(err)
				assert.Equal(mockUnmarshalApplicationModel, application)
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
			mockRouter := mockApplicationRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}
