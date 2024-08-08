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
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/gammazero/deque"
	"github.com/go-http-utils/headers"
	"github.com/schollz/progressbar/v3"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	dfdaemonv1 "d7y.io/api/v2/pkg/apis/dfdaemon/v1"

	"d7y.io/dragonfly/v2/client/config"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/digest"
	dfdaemonclient "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	"d7y.io/dragonfly/v2/pkg/source"
	pkgstrings "d7y.io/dragonfly/v2/pkg/strings"
)

func Download(cfg *config.DfgetConfig, client dfdaemonclient.V1) error {
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
		return fmt.Errorf("download timeout(%s)", cfg.Timeout)
	}
	return downError
}

func download(ctx context.Context, client dfdaemonclient.V1, cfg *config.DfgetConfig, wLog *logger.SugaredLoggerOnWith) error {
	if cfg.Recursive {
		return recursiveDownload(ctx, client, cfg)
	}
	return singleDownload(ctx, client, cfg, wLog)
}

func singleDownload(ctx context.Context, client dfdaemonclient.V1, cfg *config.DfgetConfig, wLog *logger.SugaredLoggerOnWith) error {
	hdr := parseHeader(cfg.Header)

	if client == nil {
		return downloadFromSource(ctx, cfg, hdr)
	}

	var (
		start     = time.Now()
		stream    dfdaemonv1.Daemon_DownloadClient
		result    *dfdaemonv1.DownResult
		pb        *progressbar.ProgressBar
		request   = newDownRequest(cfg, hdr)
		downError error
	)

	if stream, downError = client.Download(ctx, request); downError != nil {
		goto processError
	}

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

			wLog.Infof("download from daemon success, length: %d bytes, cost: %d ms", result.CompletedLength, time.Since(start).Milliseconds())
			fmt.Printf("finish total length %d bytes\n", result.CompletedLength)

			break
		}
	}

processError:
	if downError != nil && !cfg.KeepOriginalOffset {
		wLog.Warnf("daemon downloads file error: %v", downError)
		fmt.Printf("daemon downloads file error: %v\n", downError)
		downError = downloadFromSource(ctx, cfg, hdr)
	}

	return downError
}

func downloadFromSource(ctx context.Context, cfg *config.DfgetConfig, hdr map[string]string) (err error) {
	if cfg.DisableBackSource {
		return errors.New("try to download from source but back source is disabled")
	}

	var (
		wLog     = logger.With("url", cfg.URL)
		start    = time.Now()
		tempFile *os.File
		response *source.Response
		written  int64
		renameOK bool
	)

	wLog.Info("try to download from source and ignore rate limit")
	fmt.Println("try to download from source and ignore rate limit")

	if tempFile, err = os.CreateTemp(filepath.Dir(cfg.Output), ".df_"); err != nil {
		return err
	}
	defer func() {
		if !renameOK {
			tempPath := path.Join(filepath.Dir(cfg.Output), tempFile.Name())
			removeErr := os.Remove(tempPath)
			if removeErr != nil {
				wLog.Infof("remove temporary file %s error: %s", tempPath, removeErr)
				fmt.Printf("remove temporary file %s error: %s\n", tempPath, removeErr)
			}
		}
		if cerr := tempFile.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()

	downloadRequest, err := source.NewRequestWithContext(ctx, cfg.URL, hdr)
	if err != nil {
		return err
	}
	if response, err = source.Download(downloadRequest); err != nil {
		return err
	}
	defer func() {
		if cerr := response.Body.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()
	if err = response.Validate(); err != nil {
		return err
	}

	if written, err = io.Copy(tempFile, response.Body); err != nil {
		return err
	}

	if !pkgstrings.IsBlank(cfg.Digest) {
		d, err := digest.Parse(cfg.Digest)
		if err != nil {
			return err
		}

		encoded, err := digest.HashFile(tempFile.Name(), d.Algorithm)
		if err != nil {
			return err
		}

		if encoded != "" && encoded != d.Encoded {
			return fmt.Errorf("%s digest is not matched: real[%s] expected[%s]", d.Algorithm, encoded, d.Encoded)
		}
	}

	// change file owner
	if err = os.Chown(tempFile.Name(), os.Getuid(), os.Getgid()); err != nil {
		return fmt.Errorf("change file owner to uid[%d] gid[%d]: %w", os.Getuid(), os.Getgid(), err)
	}

	if err = os.Rename(tempFile.Name(), cfg.Output); err != nil {
		return err
	}
	renameOK = true

	wLog.Infof("download from source success, length: %d bytes, cost: %d ms", written, time.Since(start).Milliseconds())
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

func newDownRequest(cfg *config.DfgetConfig, hdr map[string]string) *dfdaemonv1.DownRequest {
	var rg string
	if r, ok := hdr[headers.Range]; ok {
		rg = strings.TrimPrefix(r, "bytes=")
	} else {
		rg = cfg.Range
	}
	request := &dfdaemonv1.DownRequest{
		Url:               cfg.URL,
		Output:            cfg.Output,
		Timeout:           uint64(cfg.Timeout),
		Limit:             float64(cfg.RateLimit.Limit),
		DisableBackSource: cfg.DisableBackSource,
		UrlMeta: &commonv1.UrlMeta{
			Digest:      cfg.Digest,
			Tag:         cfg.Tag,
			Range:       rg,
			Filter:      cfg.Filter,
			Header:      hdr,
			Application: cfg.Application,
			Priority:    commonv1.Priority(cfg.Priority),
		},
		Uid:                int64(os.Getuid()),
		Gid:                int64(os.Getgid()),
		KeepOriginalOffset: cfg.KeepOriginalOffset,
	}

	_url, err := url.Parse(cfg.URL)
	if err == nil {
		director, ok := source.HasDirector(_url.Scheme)
		if ok {
			err = director.Direct(_url, request.UrlMeta)
			if err == nil {
				// write back new url
				request.Url = _url.String()
			} else {
				logger.Errorf("direct resource error: %s", err)
			}
		}
	}

	return request
}

func newProgressBar(max int64) *progressbar.ProgressBar {
	return progressbar.DefaultBytes(-1, "Downloading")
}

func accept(u string, accept, reject string) bool {
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

// recursiveDownload breadth-first download all resources
func recursiveDownload(ctx context.Context, client dfdaemonclient.V1, cfg *config.DfgetConfig) error {
	// if recursive level is 0, skip recursive level check
	var skipLevel bool
	if cfg.RecursiveLevel == 0 {
		skipLevel = true
	}
	var queue deque.Deque[*config.DfgetConfig]
	queue.PushBack(cfg)
	downloadMap := map[url.URL]struct{}{}
	for {
		if queue.Len() == 0 {
			break
		}
		parentCfg := queue.PopFront()
		if !skipLevel {
			if parentCfg.RecursiveLevel == 0 {
				logger.Infof("%s recursive level reached, skip", parentCfg.URL)
				continue
			}
			parentCfg.RecursiveLevel--
		}
		request, err := source.NewRequestWithContext(ctx, parentCfg.URL, parseHeader(parentCfg.Header))
		if err != nil {
			return err
		}
		// prevent loop downloading
		if _, exist := downloadMap[*request.URL]; exist {
			continue
		}
		downloadMap[*request.URL] = struct{}{}

		urlEntries, err := source.List(request)
		if err != nil {
			logger.Errorf("url [%v] source lister error: %v", request.URL, err)
		}
		for _, urlEntry := range urlEntries {
			childCfg := *parentCfg //create new cfg
			childCfg.Output = path.Join(parentCfg.Output, urlEntry.Name)
			fmt.Printf("%s\n", strings.TrimPrefix(childCfg.Output, cfg.Output))
			u := urlEntry.URL
			childCfg.URL = u.String()

			if !accept(childCfg.URL, childCfg.RecursiveAcceptRegex, childCfg.RecursiveRejectRegex) {
				logger.Infof("url %s is not accepted, skip", childCfg.URL)
				continue
			}

			if urlEntry.IsDir {
				logger.Infof("download directory %s to %s", childCfg.URL, childCfg.Output)
				queue.PushBack(&childCfg)
				continue
			}

			if childCfg.RecursiveList {
				continue
			}
			childCfg.Recursive = false
			// validate new dfget config
			if err = childCfg.Validate(); err != nil {
				logger.Errorf("validate failed: %s", err)
				return err
			}
			logger.Infof("download file %s to %s", childCfg.URL, childCfg.Output)
			if err = singleDownload(ctx, client, &childCfg, logger.With("url", childCfg.URL)); err != nil {
				return err
			}
		}
	}
	return nil
}
