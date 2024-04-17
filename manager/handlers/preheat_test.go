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

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"d7y.io/dragonfly/v2/manager/service/mocks"
	"d7y.io/dragonfly/v2/manager/types"
)

var (
	mockPreheatReqBody = `
		{
			"filteredQueryParams": "bar",
			"headers": {
				"Content-Length": "100",
				"Range": "bytes=0-99"
			},
			"type": "image",
			"url": "http://example.com/foo"
		}`
	mockCreateV1PreheatRequest = types.CreateV1PreheatRequest{
		Type:                "image",
		URL:                 "http://example.com/foo",
		FilteredQueryParams: "bar",
		Headers:             map[string]string{"Content-Length": "100", "Range": "bytes=0-99"},
	}
	mockCreateV1PreheatResponse = &types.CreateV1PreheatResponse{
		ID: "2",
	}
	mockGetV1PreheatResponse = &types.GetV1PreheatResponse{
		ID: "2",
	}
)

func mockPreheatRouter(h *Handlers) *gin.Engine {
	r := gin.Default()
	pv1 := r.Group("/preheats")
	pv1.POST("", h.CreateV1Preheat)
	pv1.GET(":id", h.GetV1Preheat)
	return r
}

func TestHandlers_CreateV1Preheat(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "unprocessable entity",
			req:  httptest.NewRequest("POST", "/preheats", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusUnprocessableEntity, w.Code)
			},
		},
		{
			name: "success",
			req:  httptest.NewRequest("POST", "/preheats", strings.NewReader(mockPreheatReqBody)),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.CreateV1Preheat(gomock.Any(), gomock.Eq(mockCreateV1PreheatRequest)).Return(mockCreateV1PreheatResponse, nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				assert.Equal(w.Body.String(), `{"id":"2"}`)
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
			mockRouter := mockPreheatRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}

func TestHandlers_GetV1Preheat(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		mock   func(ms *mocks.MockServiceMockRecorder)
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "success",
			req:  httptest.NewRequest("GET", "/preheats/2", nil),
			mock: func(ms *mocks.MockServiceMockRecorder) {
				ms.GetV1Preheat(gomock.Any(), gomock.Eq("2")).Return(mockGetV1PreheatResponse, nil).Times(1)
			},
			expect: func(t *testing.T, w *httptest.ResponseRecorder) {
				assert := assert.New(t)
				assert.Equal(http.StatusOK, w.Code)
				assert.Equal(w.Body.String(), `{"id":"2","status":""}`)
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
			mockRouter := mockPreheatRouter(h)

			tc.mock(svc.EXPECT())
			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}
