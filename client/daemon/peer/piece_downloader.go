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
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/daemon/upload"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
)

type DownloadPieceRequest struct {
	TaskID     string
	DstPid     string
	DstAddr    string
	CalcDigest bool
	piece      *base.PieceInfo
}

type PieceDownloader interface {
	DownloadPiece(*DownloadPieceRequest) (io.Reader, io.Closer, error)
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

func (p *pieceDownloader) DownloadPiece(d *DownloadPieceRequest) (io.Reader, io.Closer, error) {
	resp, err := p.httpClient.Do(buildDownloadPieceHTTPRequest(d))
	if err != nil {
		logger.Errorf("task id: %s, piece num: %d, dst: %s, download piece failed: %s",
			d.TaskID, d.piece.PieceNum, d.DstAddr, err)
		return nil, nil, err
	}
	if resp.StatusCode > 299 {
		_, _ = io.Copy(ioutil.Discard, resp.Body)
		_ = resp.Body.Close()
		return nil, nil, fmt.Errorf("download piece failed with http code: %s", resp.Status)
	}
	r := resp.Body.(io.Reader)
	c := resp.Body.(io.Closer)
	if d.CalcDigest {
		r = clientutil.NewDigestReader(io.LimitReader(resp.Body, int64(d.piece.RangeSize)), d.piece.PieceMd5)
	}
	return r, c, nil
}

func buildDownloadPieceHTTPRequest(d *DownloadPieceRequest) *http.Request {
	b := strings.Builder{}
	b.WriteString("http://")
	b.WriteString(d.DstAddr)
	b.WriteString(upload.PeerDownloadHTTPPathPrefix)
	b.Write([]byte(d.TaskID)[:3])
	b.Write([]byte("/"))
	b.WriteString(d.TaskID)
	b.Write([]byte("?peerId="))
	b.WriteString(d.DstPid)

	req, _ := http.NewRequest(http.MethodGet, b.String(), nil)

	// TODO use string.Builder
	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d",
		d.piece.RangeStart, d.piece.RangeStart+uint64(d.piece.RangeSize)-1))
	return req
}
