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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/service/mocks"
	"d7y.io/dragonfly/v2/manager/types"
)

var (
	mockPersonalAccessTokenReqBody = `
		{
			"bio": "bio",
			"expired_at": "2024-04-21T16:53:21.5804709Z",
			"name": "foo",
			"state": "active",
			"user_id": 4
		}`
	mockPersonalAccessTokenResponseBody  = `{"id":0,"created_at":"0001-01-01T00:00:00Z","updated_at":"0001-01-01T00:00:00Z","is_del":0,"name":"foo","bio":"bio","token":"","scopes":null,"state":"active","expired_at":"2024-04-21T16:53:21.5804709Z","user_id":4,"user":{"id":0,"created_at":"0001-01-01T00:00:00Z","updated_at":"0001-01-01T00:00:00Z","is_del":0,"email":"","name":"","avatar":"","phone":"","state":"","location":"","bio":"","configs":null}}`
	mockCreatePersonalAccessTokenRequest = types.CreatePersonalAccessTokenRequest{
		Name:      "foo",
		ExpiredAt: time.Date(2024, 4, 21, 16, 53, 21, 580470900, time.UTC),
		UserID:    4,
		BIO:       "bio",
	}
	mockUpdatePersonalAccessTokenRequest = types.UpdatePersonalAccessTokenRequest{
		State:     "active",
		ExpiredAt: time.Date(2024, 4, 21, 16, 53, 21, 580470900, time.UTC),
		UserID:    4,
		BIO:       "bio",
	}
	mockPersonalAccessTokenModel = &models.PersonalAccessToken{
		Name:      "foo",
		State:     "active",
		ExpiredAt: time.Date(2024, 4, 21, 16, 53, 21, 580470900, time.UTC),
		UserID:    4,
		BIO:       "bio",
	}
)

func mockPersonalAccessTokenRouter(h *Handlers) *gin.Engine {
	r := gin.Default()
	apiv1 := r.Group("/api/v1")
	pat := apiv1.Group("/personal-access-tokens")
	pat.POST("", h.CreatePersonalAccessToken)
	pat.DELETE(":id", h.DestroyPersonalAccessToken)
	pat.PATCH(":id", h.UpdatePersonalAccessToken)
	pat.GET(":id", h.GetPersonalAccessToken)
	pat.GET("", h.GetPersonalAccessTokens)
	return r
}

func TestHandlers_CreatePersonalAccessToken(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity",
			req:  httptest.NewRequest("POST", "/api/v1/personal-access-tokens", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest("POST", "/api/v1/personal-access-tokens", strings.NewReader(mockPersonalAccessTokenReqBody)),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.CreatePersonalAccessToken(gomock.Any(), gomock.Eq(mockCreatePersonalAccessTokenRequest)).Return(mockPersonalAccessTokenModel, nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				assert.Equal(w.Body.String(), mockPersonalAccessTokenResponseBody)
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
			mockRouter := mockPersonalAccessTokenRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_DestroyPersonalAccessToken(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity",
			req:  httptest.NewRequest("DELETE", "/api/v1/personal-access-tokens/test", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest("DELETE", "/api/v1/personal-access-tokens/2", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.DestroyPersonalAccessToken(gomock.Any(), gomock.Eq(uint(2))).Return(nil).Times(1)
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
			mockRouter := mockPersonalAccessTokenRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_UpdatePersonalAccessToken(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity caused by uri",
			req:  httptest.NewRequest("PATCH", "/api/v1/personal-access-tokens/test", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "unprocessable entity caused by body",
			req:  httptest.NewRequest("PATCH", "/api/v1/personal-access-tokens/2", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest("PATCH", "/api/v1/personal-access-tokens/2", strings.NewReader(mockPersonalAccessTokenReqBody)),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.UpdatePersonalAccessToken(gomock.Any(), gomock.Eq(uint(2)), gomock.Eq(mockUpdatePersonalAccessTokenRequest)).Return(mockPersonalAccessTokenModel, nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				assert.Equal(w.Body.String(), mockPersonalAccessTokenResponseBody)
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
			mockRouter := mockPersonalAccessTokenRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_GetPersonalAccessToken(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity",
			req:  httptest.NewRequest("GET", "/api/v1/personal-access-tokens/test", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest("GET", "/api/v1/personal-access-tokens/2", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.GetPersonalAccessToken(gomock.Any(), gomock.Eq(uint(2))).Return(mockPersonalAccessTokenModel, nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				assert.Equal(w.Body.String(), mockPersonalAccessTokenResponseBody)
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
			mockRouter := mockPersonalAccessTokenRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_GetPersonalAccessTokens(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity",
			req:  httptest.NewRequest("GET", "/api/v1/personal-access-tokens?page=-1", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest("GET", "/api/v1/personal-access-tokens?user_id=4", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.GetPersonalAccessTokens(gomock.Any(), gomock.Eq(types.GetPersonalAccessTokensQuery{
					UserID:  4,
					Page:    1,
					PerPage: 10,
				})).Return([]models.PersonalAccessToken{*mockPersonalAccessTokenModel}, int64(1), nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				assert.Equal(w.Body.String(), "["+mockPersonalAccessTokenResponseBody+"]")
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
			mockRouter := mockPersonalAccessTokenRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}
