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

package dfget

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/go-http-utils/headers"
	"github.com/pkg/errors"
	"github.com/schollz/progressbar/v3"

	"d7y.io/dragonfly/v2/client/config"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/basic"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	daemonclient "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
)

func Download(cfg *config.DfgetConfig, client daemonclient.DaemonClient) error {
	var (
		ctx       = context.Background()
		cancel    context.CancelFunc
		wLog      = logger.With("url", cfg.URL)
		downError error
	)

	wLog.Info("init success and start to download")
	fmt.Println("init success and start to download")

	if cfg.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, cfg.Timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

	go func() {
		downError = download(ctx, client, cfg, wLog)
		cancel()
	}()

	<-ctx.Done()

	if ctx.Err() == context.DeadlineExceeded {
		return errors.Errorf("download timeout(%s)", cfg.Timeout)
	}
	return downError
}

func download(ctx context.Context, client daemonclient.DaemonClient, cfg *config.DfgetConfig, wLog *logger.SugaredLoggerOnWith) error {
	if cfg.Recursive {
		return recursiveDownload(ctx, client, cfg)
	}
	return singleDownload(ctx, client, cfg, wLog)
}

func singleDownload(ctx context.Context, client daemonclient.DaemonClient, cfg *config.DfgetConfig, wLog *logger.SugaredLoggerOnWith) error {
	hdr := parseHeader(cfg.Header)

	if client == nil {
		return downloadFromSource(ctx, cfg, hdr)
	}

	var (
		start     = time.Now()
		stream    *daemonclient.DownResultStream
		result    *dfdaemon.DownResult
		pb        *progressbar.ProgressBar
		request   = newDownRequest(cfg, hdr)
		downError error
	)

	if stream, downError = client.Download(ctx, request); downError == nil {
		if cfg.ShowProgress {
			pb = newProgressBar(-1)
		}

		for {
			if result, downError = stream.Recv(); downError != nil {
				break
			}

			if result.CompletedLength > 0 && pb != nil {
				_ = pb.Set64(int64(result.CompletedLength))
			}

			// success
			if result.Done {
				if pb != nil {
					pb.Describe("Downloaded")
					_ = pb.Close()
				}

				wLog.Infof("download from daemon success, length: %d bytes cost: %d ms", result.CompletedLength, time.Since(start).Milliseconds())
				fmt.Printf("finish total length %d bytes\n", result.CompletedLength)

				break
			}
		}
	}

	if downError != nil && !cfg.KeepOriginalOffset {
		wLog.Warnf("daemon downloads file error: %v", downError)
		fmt.Printf("daemon downloads file error: %v\n", downError)
		downError = downloadFromSource(ctx, cfg, hdr)
	}

	return downError
}

func downloadFromSource(ctx context.Context, cfg *config.DfgetConfig, hdr map[string]string) error {
	if cfg.DisableBackSource {
		return errors.New("try to download from source but back source is disabled")
	}

	var (
		wLog     = logger.With("url", cfg.URL)
		start    = time.Now()
		target   *os.File
		response *source.Response
		err      error
		written  int64
	)

	wLog.Info("try to download from source and ignore rate limit")
	fmt.Println("try to download from source and ignore rate limit")

	if target, err = os.CreateTemp(filepath.Dir(cfg.Output), ".df_"); err != nil {
		return err
	}
	defer os.Remove(target.Name())
	defer target.Close()

	downloadRequest, err := source.NewRequestWithContext(ctx, cfg.URL, hdr)
	if err != nil {
		return err
	}
	if response, err = source.Download(downloadRequest); err != nil {
		return err
	}
	defer response.Body.Close()
	if err = response.Validate(); err != nil {
		return err
	}

	if written, err = io.Copy(target, response.Body); err != nil {
		return err
	}

	if !stringutils.IsBlank(cfg.Digest) {
		parsedHash := digestutils.Parse(cfg.Digest)
		realHash := digestutils.HashFile(target.Name(), digestutils.Algorithms[parsedHash[0]])

		if realHash != "" && realHash != parsedHash[1] {
			return errors.Errorf("%s digest is not matched: real[%s] expected[%s]", parsedHash[0], realHash, parsedHash[1])
		}
	}

	// change file owner
	if err = os.Chown(target.Name(), basic.UserID, basic.UserGroup); err != nil {
		return errors.Wrapf(err, "change file owner to uid[%d] gid[%d]", basic.UserID, basic.UserGroup)
	}

	if err = os.Rename(target.Name(), cfg.Output); err != nil {
		return err
	}

	wLog.Infof("download from source success, length: %d bytes cost: %d ms", written, time.Since(start).Milliseconds())
	fmt.Printf("finish total length %d bytes\n", written)

	return nil
}

func parseHeader(s []string) map[string]string {
	hdr := make(map[string]string)
	var key, value string
	for _, h := range s {
		idx := strings.Index(h, ":")
		if idx > 0 {
			key = strings.TrimSpace(h[:idx])
			value = strings.TrimSpace(h[idx+1:])
			hdr[key] = value
		}
	}

	return hdr
}

func newDownRequest(cfg *config.DfgetConfig, hdr map[string]string) *dfdaemon.DownRequest {
	var rg string
	if r, ok := hdr[headers.Range]; ok {
		rg = strings.TrimLeft(r, "bytes=")
	} else {
		rg = cfg.Range
	}
	return &dfdaemon.DownRequest{
		Url:               cfg.URL,
		Output:            cfg.Output,
		Timeout:           uint64(cfg.Timeout),
		Limit:             float64(cfg.RateLimit.Limit),
		DisableBackSource: cfg.DisableBackSource,
		UrlMeta: &base.UrlMeta{
			Digest: cfg.Digest,
			Tag:    cfg.Tag,
			Range:  rg,
			Filter: cfg.Filter,
			Header: hdr,
		},
		Pattern:            cfg.Pattern,
		Callsystem:         cfg.CallSystem,
		Uid:                int64(basic.UserID),
		Gid:                int64(basic.UserGroup),
		KeepOriginalOffset: cfg.KeepOriginalOffset,
	}
}

func newProgressBar(max int64) *progressbar.ProgressBar {
	return progressbar.NewOptions64(max,
		progressbar.OptionShowBytes(true),
		progressbar.OptionShowIts(),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionUseANSICodes(true),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetDescription("[cyan]Downloading...[reset]"),
		progressbar.OptionSetRenderBlankState(true),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))
}

func accept(u string, parent, sub string, level uint, accept, reject string) bool {
	if !checkDirectoryLevel(parent, sub, level) {
		logger.Debugf("url %s not accept by level: %d", u, level)
		return false
	}
	if !acceptRegex(u, accept) {
		logger.Debugf("url %s not accept by regex: %s", u, accept)
		return false
	}
	if rejectRegex(u, reject) {
		logger.Debugf("url %s rejected by regex: %s", u, reject)
		return false
	}
	return true
}

func checkDirectoryLevel(parent, sub string, level uint) bool {
	if level == 0 {
		return true
	}
	dirs := strings.Split(strings.Trim(strings.TrimLeft(sub, parent), "/"), "/")
	return uint(len(dirs)) <= level
}

func acceptRegex(u string, accept string) bool {
	if accept == "" {
		return true
	}
	return regexp.MustCompile(accept).Match([]byte(u))
}

func rejectRegex(u string, reject string) bool {
	if reject == "" {
		return false
	}
	return regexp.MustCompile(reject).Match([]byte(u))
}

func recursiveDownload(ctx context.Context, client daemonclient.DaemonClient, cfg *config.DfgetConfig) error {
	request, err := source.NewRequestWithContext(ctx, cfg.URL, parseHeader(cfg.Header))
	if err != nil {
		return err
	}
	dirURL, err := url.Parse(cfg.URL)
	if err != nil {
		return err
	}
	logger.Debugf("dirURL: %s", cfg.URL)

	urls, err := source.List(request)
	if err != nil {
		return err
	}
	for _, u := range urls {
		// reuse dfget config
		c := *cfg
		// update some attributes
		c.Recursive, c.URL, c.Output = false, u.String(), path.Join(cfg.Output, strings.TrimPrefix(u.Path, dirURL.Path))
		if !accept(c.URL, dirURL.Path, u.Path, cfg.RecursiveLevel, cfg.RecursiveAcceptRegex, cfg.RecursiveRejectRegex) {
			logger.Debugf("url %s is not accepted, skip", c.URL)
			continue
		}
		if cfg.RecursiveList {
			fmt.Printf("%s\n", u.String())
			continue
		}
		// validate new dfget config
		if err = c.Validate(); err != nil {
			logger.Errorf("validate failed: %s", err)
			return err
		}

		logger.Debugf("download %s to %s", c.URL, c.Output)
		err = download(ctx, client, &c, logger.With("url", c.URL))
		if err != nil {
			return err
		}
	}
	return nil
}
