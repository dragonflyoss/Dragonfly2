/*
 *     Copyright 2020 The Dragonfly Authors
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
	"fmt"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"d7y.io/dragonfly/v2/manager/service"
)

type Handlers struct {
	service service.Service
}

func New(service service.Service) *Handlers {
	return &Handlers{
		service: service,
	}
}

func (h *Handlers) setPaginationDefault(page, perPage *int) {
	if *page == 0 {
		*page = 1
	}

	if *perPage == 0 {
		*perPage = 10
	}
}

func (h *Handlers) setPaginationLinkHeader(ctx *gin.Context, page, perPage, totalCount int) {
	totalPage := totalCount / perPage
	if totalCount%perPage > 0 {
		totalPage++
	}

	var prevPage int
	if page == 1 {
		prevPage = 1
	} else {
		prevPage = page - 1
	}

	var nextPage int
	if page == totalPage {
		nextPage = page
	} else {
		nextPage = page + 1
	}

	var links []string
	for _, v := range []struct {
		Name string
		Page int
	}{
		{
			Name: "prev",
			Page: prevPage,
		},
		{
			Name: "next",
			Page: nextPage,
		},
		{
			Name: "first",
			Page: 1,
		},
		{
			Name: "last",
			Page: totalPage,
		},
	} {
		url := ctx.Request.URL
		query := url.Query()
		query.Set("page", strconv.Itoa(v.Page))
		query.Set("per_page", strconv.Itoa(perPage))
		url.RawQuery = query.Encode()

		links = append(links, fmt.Sprintf("<%s>;rel=%s", url.String(), v.Name))
	}

	ctx.Header("Link", strings.Join(links, ","))
}
