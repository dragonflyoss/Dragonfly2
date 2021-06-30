package handlers

import (
	"fmt"
	"strconv"
	"strings"

	"d7y.io/dragonfly/v2/manager/service"
	"github.com/gin-gonic/gin"
)

type Handlers struct {
	service service.REST
}

func New(service service.REST) *Handlers {
	return &Handlers{service: service}
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
	if totalPage == 0 {
		totalPage = 1
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
