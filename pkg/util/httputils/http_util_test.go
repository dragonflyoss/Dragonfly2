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

package httputils

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"github.com/stretchr/testify/suite"
	"github.com/valyala/fasthttp"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHttpUtil(t *testing.T) {
	suite.Run(t, new(HTTPUtilTestSuite))
}

type HTTPUtilTestSuite struct {
	port int
	host string
	nl   net.Listener
	suite.Suite
}

func (s *HTTPUtilTestSuite) SetupSuite() {
	s.port = rand.Intn(1000) + 63000
	s.host = fmt.Sprintf("127.0.0.1:%d", s.port)
	fmt.Println(s.host)
	s.nl, _ = net.Listen("tcp", s.host)
	go fasthttp.Serve(s.nl, func(ctx *fasthttp.RequestCtx) {
		ctx.SetContentType(ApplicationJSONUtf8Value)
		ctx.SetStatusCode(fasthttp.StatusOK)
		req := &testJSONReq{}
		json.Unmarshal(ctx.Request.Body(), req)
		res := testJSONRes{
			Sum: req.A + req.B,
		}
		resByte, _ := json.Marshal(res)
		ctx.SetBody(resByte)
		time.Sleep(50 * time.Millisecond)
	})
}

func (s *HTTPUtilTestSuite) TearDownSuite() {
	s.nl.Close()
}

// ----------------------------------------------------------------------------
// unit tests

func (s *HTTPUtilTestSuite) TestPostJson() {
	code, body, e := PostJSON("http://"+s.host, req(1, 2), 60*time.Millisecond)
	checkOk(s, code, body, e, 3)

	_, _, e = PostJSON("http://"+s.host, req(1, 2), 50*time.Millisecond)
	s.EqualError(e, "timeout")

	code, body, e = PostJSON("http://"+s.host, req(2, 3), 0)
	checkOk(s, code, body, e, 5)

	code, body, e = PostJSON("http://"+s.host, nil, 0)
	checkOk(s, code, body, e, 0)
}

func (s *HTTPUtilTestSuite) TestGet() {
	code, body, e := Get("http://"+s.host, 0)
	checkOk(s, code, body, e, 0)

	_, _, e = Get("http://"+s.host, 50*time.Millisecond)
	s.EqualError(e, "timeout")
}

func (s *HTTPUtilTestSuite) TestHttpGet() {
	res, e := HTTPGetTimeout("http://"+s.host, nil, 0)
	s.Nil(e)
	code := res.StatusCode
	body, e := ioutil.ReadAll(res.Body)
	s.Nil(e)
	res.Body.Close()

	checkOk(s, code, body, e, 0)

	res, e = HTTPGetTimeout("http://"+s.host, nil, 60*time.Millisecond)
	s.Nil(e)
	code = res.StatusCode
	body, e = ioutil.ReadAll(res.Body)
	s.Nil(e)
	res.Body.Close()

	checkOk(s, code, body, e, 0)

	_, e = HTTPGetTimeout("http://"+s.host, nil, 20*time.Millisecond)
	s.NotNil(e)
	s.Equal(strings.Contains(e.Error(), context.DeadlineExceeded.Error()), true)
}

func (s *HTTPUtilTestSuite) TestGetRangeSE() {
	var cases = []struct {
		rangeHTTPHeader string
		length          int64
		expected        []*RangeStruct
		errCheck        func(error) bool
	}{
		{
			rangeHTTPHeader: "bytes=0-65575",
			length:          65576,
			expected: []*RangeStruct{
				{
					StartIndex: 0,
					EndIndex:   65575,
				},
			},
			errCheck: cdnerrors.IsNilError,
		},
		{
			rangeHTTPHeader: "bytes=2-2",
			length:          65576,
			expected: []*RangeStruct{
				{
					StartIndex: 2,
					EndIndex:   2,
				},
			},
			errCheck: cdnerrors.IsNilError,
		},
		{
			rangeHTTPHeader: "bytes=2-",
			length:          65576,
			expected: []*RangeStruct{
				{
					StartIndex: 2,
					EndIndex:   65575,
				},
			},
			errCheck: cdnerrors.IsNilError,
		},
		{
			rangeHTTPHeader: "bytes=-100",
			length:          65576,
			expected: []*RangeStruct{
				{
					StartIndex: 65476,
					EndIndex:   65575,
				},
			},
			errCheck: cdnerrors.IsNilError,
		},
		{
			rangeHTTPHeader: "bytes=0-66575",
			length:          65576,
			expected:        nil,
			errCheck:        cdnerrors.IsRangeNotSatisfiable,
		},
		{
			rangeHTTPHeader: "bytes=0-65-575",
			length:          65576,
			expected:        nil,
			errCheck:        cdnerrors.IsInvalidValue,
		},
		{
			rangeHTTPHeader: "bytes=0-hello",
			length:          65576,
			expected:        nil,
			errCheck:        cdnerrors.IsInvalidValue,
		},
		{
			rangeHTTPHeader: "bytes=65575-0",
			length:          65576,
			expected:        nil,
			errCheck:        cdnerrors.IsInvalidValue,
		},
		{
			rangeHTTPHeader: "bytes=-1-8",
			length:          65576,
			expected:        nil,
			errCheck:        cdnerrors.IsInvalidValue,
		},
	}

	for _, v := range cases {
		result, err := GetRange(v.rangeHTTPHeader, v.length)
		s.Equal(v.errCheck(err), true)
		fmt.Println(v.rangeHTTPHeader)
		s.Equal(result, v.expected)
	}
}

func (s *HTTPUtilTestSuite) TestConcurrencyPostJson() {
	wg := &sync.WaitGroup{}
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func(x, y int) {
			defer wg.Done()
			code, body, e := PostJSON("http://"+s.host, req(x, y), 1*time.Second)
			time.Sleep(20 * time.Millisecond)
			checkOk(s, code, body, e, x+y)
		}(i, i)
	}

	wg.Wait()
}

func (s *HTTPUtilTestSuite) TestConstructRangeStr() {
	s.Equal(ConstructRangeStr("200-1000"), "bytes=200-1000")
}


// ----------------------------------------------------------------------------
// helper functions and structures

func checkOk(s *HTTPUtilTestSuite, code int, body []byte, e error, sum int) {
	s.Nil(e)
	s.Equal(code, fasthttp.StatusOK)

	var res = &testJSONRes{}
	e = json.Unmarshal(body, res)
	s.Nil(e)
	s.Equal(res.Sum, sum)
}

func req(x int, y int) *testJSONReq {
	return &testJSONReq{x, y}
}

type testJSONReq struct {
	A int
	B int
}

type testJSONRes struct {
	Sum int
}

type testTransport struct {
}

func (t *testTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Body:          http.NoBody,
		Status:        http.StatusText(http.StatusOK),
		StatusCode:    http.StatusOK,
		ContentLength: -1,
	}, nil
}

func (s *HTTPUtilTestSuite) TestRegisterProtocol() {
	protocol := "test"
	RegisterProtocol(protocol, &testTransport{})
	resp, err := HTTPWithHeaders(http.MethodGet,
		protocol+"://test/test",
		map[string]string{
			"test": "test",
		},
		time.Second,
		&tls.Config{},
	)
	s.Nil(err)
	defer resp.Body.Close()

	s.NotNil(resp)
	buf := make([]byte,0)
	resp.Body.Read(buf)
	fmt.Println(string(buf))
	fmt.Println(resp.ContentLength)
	s.Equal(resp.ContentLength, int64(-1))
}
