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

package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/client/config"
)

type testItem struct {
	URL      string
	Direct   bool
	UseHTTPS bool
	Redirect string
}

type testCase struct {
	Error          error
	Rules          []*config.ProxyRule
	RegistryMirror *config.RegistryMirror
	Items          []testItem
}

func newTestCase() *testCase {
	return &testCase{}
}

func (tc *testCase) WithRule(regx string, direct bool, useHTTPS bool, redirect string) *testCase {
	if tc.Error != nil {
		return tc
	}

	var r *config.ProxyRule
	r, tc.Error = config.NewProxyRule(regx, useHTTPS, direct, redirect)
	tc.Rules = append(tc.Rules, r)
	return tc
}

func (tc *testCase) WithRegistryMirror(rawURL string, direct bool, dynamic bool, useProxies bool) *testCase {
	if tc.Error != nil {
		return tc
	}

	var u *url.URL
	u, tc.Error = url.Parse(rawURL)
	tc.RegistryMirror = &config.RegistryMirror{
		Remote:        &config.URL{URL: u},
		DynamicRemote: dynamic,
		Direct:        direct,
		UseProxies:    useProxies,
	}
	return tc
}

func (tc *testCase) WithTest(url string, direct bool, useHTTPS bool, redirect string) *testCase {
	tc.Items = append(tc.Items, testItem{url, direct, useHTTPS, redirect})
	return tc
}

func (tc *testCase) Test(t *testing.T) {
	a := assert.New(t)
	if !a.Nil(tc.Error) {
		return
	}
	tp, err := NewProxy(WithRules(tc.Rules))
	if !a.Nil(err) {
		return
	}
	for _, item := range tc.Items {
		req, err := http.NewRequest("GET", item.URL, nil)
		if !a.Nil(err) {
			continue
		}
		if !a.Equal(tp.shouldUseDragonfly(req), !item.Direct) {
			fmt.Println(item.URL)
		}
		if item.UseHTTPS {
			a.Equal(req.URL.Scheme, "https")
		} else {
			a.Equal(req.URL.Scheme, "http")
		}
		if item.Redirect != "" {
			a.Equal(item.Redirect, req.URL.String())
		}
	}
}

func (tc *testCase) TestMirror(t *testing.T) {
	a := assert.New(t)
	if !a.Nil(tc.Error) {
		return
	}
	tp, err := NewProxy(WithRegistryMirror(tc.RegistryMirror), WithRules(tc.Rules))
	if !a.Nil(err) {
		return
	}
	for _, item := range tc.Items {
		req, err := http.NewRequest("GET", item.URL, nil)
		if !a.Nil(err) {
			continue
		}
		if !a.Equal(tp.shouldUseDragonflyForMirror(req), !item.Direct) {
			fmt.Println(item.URL)
		}
		if item.UseHTTPS {
			a.Equal(req.URL.Scheme, "https")
		} else {
			a.Equal(req.URL.Scheme, "http")
		}
		if item.Redirect != "" {
			a.Equal(item.Redirect, req.URL.String())
		}
	}
}

func (tc *testCase) TestEventStream(t *testing.T) {
	a := assert.New(t)
	if !a.Nil(tc.Error) {
		return
	}
	tp, err := NewProxy(WithRules(tc.Rules))
	if !a.Nil(err) {
		return
	}
	tp.transport = &mockTransport{}
	for _, item := range tc.Items {
		req, err := http.NewRequest("GET", item.URL, nil)
		if !a.Nil(err) {
			continue
		}
		if !a.Equal(tp.shouldUseDragonfly(req), !item.Direct) {
			fmt.Println(item.URL)
		}
		if item.UseHTTPS {
			a.Equal(req.URL.Scheme, "https")
		} else {
			a.Equal(req.URL.Scheme, "http")
		}
		if item.Redirect != "" {
			a.Equal(item.Redirect, req.URL.String())
		}
		if strings.Contains(req.URL.Path, "event-stream") {
			batch := 10
			_, span := tp.tracer.Start(req.Context(), config.SpanProxy)
			w := &mockResponseWriter{}
			req.Header.Set("X-Response-Batch", strconv.Itoa(batch))
			if req.URL.Path == "/event-stream" {
				req.Header.Set("X-Event-Stream", "true")
				req.Header.Set("X-Response-Content-Length", "-1")
				req.Header.Set("X-Response-Content-Encoding", "chunked")
				req.Header.Set("X-Response-Content-Type", "text/event-stream")
				tp.handleHTTP(span, w, req)
				a.GreaterOrEqual(w.flushCount, batch)
			} else {
				req.Header.Set("X-Event-Stream", "false")
				req.Header.Set("X-Response-Content-Length", strconv.Itoa(batch))
				req.Header.Set("X-Response-Content-Encoding", "")
				req.Header.Set("X-Response-Content-Type", "application/octet-stream")
				tp.handleHTTP(span, w, req)
				a.Less(w.flushCount, batch)
			}
		}
	}
}

func TestMatch(t *testing.T) {
	newTestCase().
		WithRule("/blobs/sha256/", false, false, "").
		WithTest("http://index.docker.io/v2/blobs/sha256/xxx", false, false, "").
		WithTest("http://index.docker.io/v2/auth", true, false, "").
		Test(t)

	newTestCase().
		WithRule("/a/d", false, true, "").
		WithRule("/a/b", true, false, "").
		WithRule("/a", false, false, "").
		WithRule("/a/c", true, false, "").
		WithRule("/a/e", false, true, "").
		WithTest("http://h/a", false, false, "").   // should match /a
		WithTest("http://h/a/b", true, false, "").  // should match /a/b
		WithTest("http://h/a/c", false, false, ""). // should match /a, not /a/c
		WithTest("http://h/a/d", false, true, "").  // should match /a/d and use https
		WithTest("http://h/a/e", false, false, ""). // should match /a, not /a/e
		Test(t)

	newTestCase().
		WithRule("/a/f", false, false, "r").
		WithTest("http://h/a/f", false, false, "http://r/a/f"). // should match /a/f and redirect
		Test(t)

	newTestCase().
		WithTest("http://h/a", true, false, "").
		TestMirror(t)

	newTestCase().
		WithRegistryMirror("http://index.docker.io", false, false, false).
		WithTest("http://h/a", true, false, "").
		TestMirror(t)

	newTestCase().
		WithRegistryMirror("http://index.docker.io", false, false, false).
		WithTest("http://index.docker.io/v2/blobs/sha256/xxx", false, false, "").
		TestMirror(t)

	newTestCase().
		WithRegistryMirror("http://index.docker.io", true, false, false).
		WithTest("http://index.docker.io/v2/blobs/sha256/xxx", true, false, "").
		TestMirror(t)
}

func TestMatchWithUseProxies(t *testing.T) {
	// should direct as registry is set with direct=false and no proxies are defined
	newTestCase().
		WithRegistryMirror("http://index.docker.io", false, false, true).
		WithTest("http://index.docker.io/v2/blobs/sha256/1", true, false, "").
		TestMirror(t)

	// should cache as registry is set with direct=false, and one proxy matches
	newTestCase().
		WithRegistryMirror("http://index.docker.io", false, false, true).
		WithRule("/blobs/sha256/", false, false, "").
		WithTest("http://index.docker.io/v2/blobs/sha256/2", false, false, "").
		TestMirror(t)

	// should direct as registry is set with direct=true, even if one proxy matches
	newTestCase().
		WithRegistryMirror("http://index.docker.io", true, false, true).
		WithRule("/blobs/sha256/", false, false, "").
		WithTest("http://index.docker.io/v2/blobs/sha256/3", true, false, "").
		TestMirror(t)
}

func TestMatchWithRedirect(t *testing.T) {
	// redirect as hostname, in direct mode
	newTestCase().
		WithRegistryMirror("http://index.docker.io", false, false, true).
		WithRule("index.docker.io", false, false, "r").
		WithTest("http://index.docker.io/1", false, false, "http://r/1").
		TestMirror(t)

	// redirect as hostname, in proxy mode
	newTestCase().
		WithRule("index.docker.io", false, false, "r").
		WithTest("http://index.docker.io/2", false, false, "http://r/2").
		Test(t)

	// redirect as url, in direct mode
	newTestCase().
		WithRegistryMirror("http://index.docker.io", false, false, true).
		WithRule("^http://index.docker.io/(.*)", false, false, "http://r/t/$1").
		WithTest("http://index.docker.io/1", false, false, "http://r/t/1").
		TestMirror(t)

	// redirect as url, in proxy mode
	newTestCase().
		WithRule("^http://index.docker.io/(.*)", false, false, "http://r/t/$1").
		WithTest("http://index.docker.io/2", false, false, "http://r/t/2").
		Test(t)

	// redirect as url, in direct mode, with HTTPS rewrite
	newTestCase().
		WithRegistryMirror("http://index.docker.io", false, false, true).
		WithRule("^http://index.docker.io/(.*)", false, false, "https://r/t/$1").
		WithTest("http://index.docker.io/1", false, true, "https://r/t/1").
		TestMirror(t)

}

func TestProxyEventStream(t *testing.T) {
	newTestCase().
		WithRule("/blobs/sha256/", false, false, "").
		WithTest("http://h/event-stream", true, false, "").
		WithTest("http://h/not-event-stream", true, false, "").
		TestEventStream(t)
}

type mockResponseWriter struct {
	flushCount int
}

func (w *mockResponseWriter) Header() http.Header {
	return http.Header{}
}

func (w *mockResponseWriter) Write(p []byte) (int, error) {
	return len(string(p)), nil
}

func (w *mockResponseWriter) WriteHeader(int) {}

func (w *mockResponseWriter) Flush() {
	w.flushCount++
}

type mockTransport struct{}

func (rt *mockTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	batch, _ := strconv.Atoi(r.Header.Get("X-Response-Batch"))
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &mockReadCloser{batch: batch},
		Header: http.Header{
			"Content-Length":   []string{r.Header.Get("X-Response-Content-Length")},
			"Content-Encoding": []string{r.Header.Get("X-Response-Content-Encoding")},
			"Content-Type":     []string{r.Header.Get("X-Response-Content-Type")},
		},
	}, nil
}

type mockReadCloser struct {
	batch int
	count int
}

func (rc *mockReadCloser) Read(p []byte) (n int, err error) {
	if rc.count == rc.batch {
		return 0, io.EOF
	}
	time.Sleep(100 * time.Millisecond)
	p[0] = '0'
	p = p[:1]
	rc.count++
	return len(p), nil
}

func (rc *mockReadCloser) Close() error {
	return nil
}
