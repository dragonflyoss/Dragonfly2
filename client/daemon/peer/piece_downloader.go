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

package peer

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/dragonflyoss/Dragonfly2/client/daemon/upload"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
)

type DownloadPieceRequest struct {
	TaskID string
	*base.PieceTask
}

type PieceDownloader interface {
	DownloadPiece(*DownloadPieceRequest) (io.ReadCloser, error)
}

type pieceDownloader struct {
	transport  http.RoundTripper
	httpClient *http.Client
}

var defaultTransport http.RoundTripper = &http.Transport{
	// Proxy: http.ProxyFromEnvironment,
	DialContext: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		DualStack: true,
	}).DialContext,
	ForceAttemptHTTP2:     true,
	MaxIdleConns:          100,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
}

func NewPieceDownloader(opts ...func(*pieceDownloader) error) (PieceDownloader, error) {
	pd := &pieceDownloader{}
	for _, opt := range opts {
		if err := opt(pd); err != nil {
			return nil, err
		}
	}
	if pd.transport == nil {
		pd.transport = defaultTransport
	}
	pd.httpClient = &http.Client{
		Transport: pd.transport,
		Timeout:   10 * time.Second,
	}
	return pd, nil
}

func WithTransport(rt http.RoundTripper) func(*pieceDownloader) error {
	return func(d *pieceDownloader) error {
		d.transport = rt
		return nil
	}
}

func (p *pieceDownloader) DownloadPiece(d *DownloadPieceRequest) (io.ReadCloser, error) {
	resp, err := p.httpClient.Do(p.buildHTTPRequest(d))
	if err != nil {
		logger.Errorf("task id: %s, piece num: %d, dst: %s, download piece failed: %s",
			d.TaskID, d.PieceNum, d.DstAddr, err)
		return nil, err
	}
	if resp.StatusCode > 299 {
		return nil, fmt.Errorf("download piece failed with http code: %s", resp.Status)
	}
	return resp.Body, nil
}

func (p *pieceDownloader) buildHTTPRequest(d *DownloadPieceRequest) *http.Request {
	b := strings.Builder{}
	b.WriteString("http://")
	b.WriteString(d.DstAddr)
	b.WriteString(upload.PeerDownloadHTTPPathPrefix)
	b.Write([]byte(d.TaskID)[:3])
	b.Write([]byte("/"))
	b.WriteString(d.TaskID)

	req, _ := http.NewRequest(http.MethodGet, b.String(), nil)

	// TODO use string.Builder
	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d",
		d.RangeStart, d.RangeStart+uint64(d.RangeSize)-1))
	return req
}
