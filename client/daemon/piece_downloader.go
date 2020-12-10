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

package daemon

import (
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/scheduler"
)

type DownloadPieceRequest struct {
	TaskID string
	*scheduler.PiecePackage_PieceTask
}

type PieceDownloader interface {
	DownloadPiece(*DownloadPieceRequest) (io.ReadCloser, error)
}

type pieceDownloader struct {
	transport http.RoundTripper
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

var defaultHTTPClient http.Client = http.Client{
	Transport:     defaultTransport,
	CheckRedirect: nil,
	Jar:           nil,
	Timeout:       0,
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
	return pd, nil
}

func WithTransport(rt http.RoundTripper) func(*pieceDownloader) error {
	return func(d *pieceDownloader) error {
		d.transport = rt
		return nil
	}
}

func (p *pieceDownloader) DownloadPiece(d *DownloadPieceRequest) (io.ReadCloser, error) {
	// TODO custom timeout for the request
	client := http.Client{
		Transport:     p.transport,
		CheckRedirect: nil,
		Jar:           nil,
		Timeout:       0,
	}
	resp, err := client.Do(p.buildHTTPRequest(d))
	if err != nil {
		logrus.WithFields(
			logrus.Fields{
				"taskID":   d.TaskID,
				"pieceNum": d.PieceNum,
				"dst":      d.DstAddr,
			}).Errorf("download piece failed: %s", err)
		return nil, err
	}
	return resp.Body, nil
}

func (p *pieceDownloader) buildHTTPRequest(d *DownloadPieceRequest) *http.Request {
	b := strings.Builder{}
	b.WriteString("http://")
	b.WriteString(d.DstAddr)
	b.WriteString("/peer/file/")
	b.WriteString(d.TaskID)

	req, _ := http.NewRequest(http.MethodGet, b.String(), nil)
	req.Header.Add("Range", d.PieceRange)
	return req
}
