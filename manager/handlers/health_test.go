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
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"d7y.io/dragonfly/v2/manager/service/mocks"
)

func mockHealthRouter(h *Handlers) *gin.Engine {
	r := gin.Default()
	r.GET("/healthy", h.GetHealth)
	return r
}

func TestHandlers_GetHealth(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		expect func(t *testing.T, w *httptest.ResponseRecorder)
	}{
		{
			name: "success",
			req:  httptest.NewRequest(http.MethodGet, "/healthy", nil),
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
			mockRouter := mockHealthRouter(h)

			mockRouter.ServeHTTP(w, tc.req)
			tc.expect(t, w)
		})
	}
}
