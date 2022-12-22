/*
 *     Copyright 2022 The Dragonfly Authors
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

package source

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gammazero/deque"
	"github.com/go-http-utils/headers"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

var (
	_                        ResourceClient = (*ListMetadataClient)(nil)
	ListMetadataScheme                      = "d7ylist"
	ListMetadataOriginScheme                = "X-Dragonfly-List-Origin-Scheme"
	ListMetadataExpire                      = "X-Dragonfly-List-Expire"
	tracer                   trace.Tracer
)

func init() {
	tracer = otel.Tracer("back-source")

	lmc := &ListMetadataClient{}

	if err := Register(ListMetadataScheme, lmc, lmc.adaptor); err != nil {
		panic(err)
	}
}

type ListMetadataClient struct {
}

type ListMetadata struct {
	URLEntries []URLEntry
}

func NewListMetadataRequest(request *Request) *Request {
	return nil
}

func (lm *ListMetadataClient) adaptor(request *Request) *Request {
	clonedRequest := request.Clone(request.Context())
	// set original scheme
	clonedRequest.URL.Scheme = request.Header.Get(ListMetadataOriginScheme)
	clonedRequest.Header.Del(ListMetadataOriginScheme)

	clonedRequest.URL.Path = strings.TrimPrefix(clonedRequest.URL.Path, "/")
	return clonedRequest
}

func (lm *ListMetadataClient) GetContentLength(request *Request) (int64, error) {
	return UnknownSourceFileLen, ErrClientNotSupportContentLength
}

func (lm *ListMetadataClient) IsSupportRange(request *Request) (bool, error) {
	return false, nil
}

func (lm *ListMetadataClient) IsExpired(request *Request, info *ExpireInfo) (bool, error) {
	// TODO
	return false, nil
}

func (lm *ListMetadataClient) Download(request *Request) (*Response, error) {
	ctx, span := tracer.Start(request.Context(), "list", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	traceID := span.SpanContext().TraceID()
	logKV := []any{
		"parent", request.URL.String(),
		"action", "list",
	}
	if traceID.IsValid() {
		logKV = append(logKV, "trace", traceID.String())
	}

	log := logger.With(logKV...)

	var (
		queue            deque.Deque[*Request]
		start            = time.Now()
		listMilliseconds int64
		allURLEntries    []URLEntry
	)

	queue.PushBack(request.Clone(ctx))
	downloadMap := map[url.URL]struct{}{}

	for {
		if queue.Len() == 0 {
			break
		}

		parentReq := queue.PopFront()

		// prevent loop listing
		if _, exist := downloadMap[*parentReq.URL]; exist {
			continue
		}
		downloadMap[*parentReq.URL] = struct{}{}

		listStart := time.Now()
		urlEntries, err := List(parentReq)
		if err != nil {
			log.Errorf("url [%v] source list error: %v", parentReq.URL, err)
			span.RecordError(err)
			return nil, err
		}
		cost := time.Now().Sub(listStart).Milliseconds()
		listMilliseconds += cost
		log.Infof("list dir %s cost: %dms, entries(include dir): %d", parentReq.URL, cost, len(urlEntries))

		for _, urlEntry := range urlEntries {
			if urlEntry.IsDir {
				childReq := parentReq.Clone(ctx)
				childReq.URL = urlEntry.URL
				queue.PushBack(childReq)
				continue
			}

			allURLEntries = append(allURLEntries, urlEntry)
		}
	}

	metadata := ListMetadata{
		URLEntries: allURLEntries,
	}
	data, _ := json.Marshal(metadata)

	log.Infof("list dirs cost: %dms, entries: %d, metadata size: %d, download cost: %dms",
		listMilliseconds, len(allURLEntries), len(data), time.Now().Sub(start).Milliseconds())
	return &Response{
		Status:     "OK",
		StatusCode: http.StatusOK,
		Header: map[string][]string{
			headers.Expires: {request.Header.Get(ListMetadataExpire)},
		},
		Body:          io.NopCloser(bytes.NewBuffer(data)),
		ContentLength: int64(len(data)),
		Validate: func() error {
			return nil
		},
	}, nil
}

func (lm *ListMetadataClient) GetLastModified(request *Request) (int64, error) {
	// TODO
	return 0, nil
}
