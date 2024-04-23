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
)

func TestNew(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, s any)
	}{
		{
			name: "new handler",
			expect: func(t *testing.T, h any) {
				assert := assert.New(t)
				assert.NotNil(t, h)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			svc := mocks.NewMockService(ctl)
			h := New(svc)

			tc.expect(t, h)
		})
	}
}

func TestHandlers_setPaginationDefault(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, h *Handlers)
	}{
		{
			name: "use default value",
			expect: func(t *testing.T, h *Handlers) {
				assert := assert.New(t)
				page, perpage := 0, 0
				h.setPaginationDefault(&page, &perpage)
				assert.Equal(1, page)
				assert.Equal(10, perpage)
			},
		},
		{
			name: "use customized value",
			expect: func(t *testing.T, h *Handlers) {
				assert := assert.New(t)
				page, perpage := 5, 20
				h.setPaginationDefault(&page, &perpage)
				assert.Equal(5, page)
				assert.Equal(20, perpage)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			svc := mocks.NewMockService(ctl)
			h := New(svc)

			tc.expect(t, h)
		})
	}
}

func TestHandlers_setPaginationLinkHeader(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, h *Handlers, c *gin.Context)
	}{
		{
			name: "first page",
			expect: func(t *testing.T, h *Handlers, c *gin.Context) {
				assert := assert.New(t)
				h.setPaginationLinkHeader(c, 1, 10, 95)
				linkHeader := c.Writer.Header()["Link"]
				assert.Len(linkHeader, 1)
				links := strings.Split(linkHeader[0], ",")
				assert.Len(links, 4)
				assert.Equal("</test?page=1&per_page=10>;rel=prev", links[0])
				assert.Equal("</test?page=2&per_page=10>;rel=next", links[1])
				assert.Equal("</test?page=1&per_page=10>;rel=first", links[2])
				assert.Equal("</test?page=10&per_page=10>;rel=last", links[3])
			},
		},
		{
			name: "middle page",
			expect: func(t *testing.T, h *Handlers, c *gin.Context) {
				assert := assert.New(t)
				h.setPaginationLinkHeader(c, 5, 10, 95)
				linkHeader := c.Writer.Header()["Link"]
				assert.Len(linkHeader, 1)
				links := strings.Split(linkHeader[0], ",")
				assert.Len(links, 4)
				assert.Equal("</test?page=4&per_page=10>;rel=prev", links[0])
				assert.Equal("</test?page=6&per_page=10>;rel=next", links[1])
				assert.Equal("</test?page=1&per_page=10>;rel=first", links[2])
				assert.Equal("</test?page=10&per_page=10>;rel=last", links[3])
			},
		},
		{
			name: "last page",
			expect: func(t *testing.T, h *Handlers, c *gin.Context) {
				assert := assert.New(t)
				h.setPaginationLinkHeader(c, 10, 10, 95)
				linkHeader := c.Writer.Header()["Link"]
				assert.Len(linkHeader, 1)
				links := strings.Split(linkHeader[0], ",")
				assert.Len(links, 4)
				assert.Equal("</test?page=9&per_page=10>;rel=prev", links[0])
				assert.Equal("</test?page=10&per_page=10>;rel=next", links[1])
				assert.Equal("</test?page=1&per_page=10>;rel=first", links[2])
				assert.Equal("</test?page=10&per_page=10>;rel=last", links[3])
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			svc := mocks.NewMockService(ctl)
			h := New(svc)
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request, _ = http.NewRequest("GET", "/test", nil)

			tc.expect(t, h, c)
		})
	}
}
