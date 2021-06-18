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
	"os"
	"path/filepath"
	"strings"
	"time"

	"d7y.io/dragonfly/v2/client/clientutil/progressbar"
	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/pkg/basic"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	dfdaemongrpc "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	"d7y.io/dragonfly/v2/pkg/source"

	// Init daemon rpc client
	_ "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	dfclient "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
	"github.com/go-http-utils/headers"
)

var filter string

func Download(cfg *config.DfgetConfig, client dfclient.DaemonClient) error {
	var (
		ctx    = context.Background()
		cancel context.CancelFunc
		hdr    = parseHeader(cfg.Header)
	)

	if client == nil {
		return downloadFromSource(cfg, hdr)
	}

	output, err := filepath.Abs(cfg.Output)
	if err != nil {
		return err
	}

	if cfg.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, cfg.Timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	request := &dfdaemongrpc.DownRequest{
		Url: cfg.URL,
		UrlMeta: &base.UrlMeta{
			Digest: cfg.Digest,
			Range:  hdr[headers.Range],
			Header: hdr,
		},
		Output: output,
		BizId:  cfg.CallSystem,
		Filter: filter,
		Uid:    int64(basic.UserID),
		Gid:    int64(basic.UserGroup),
	}
	var (
		start = time.Now()
		end   time.Time
	)
	down, err := client.Download(ctx, request)
	if err != nil {
		return err
	}
	var (
		result *dfdaemongrpc.DownResult
	)
	// todo using progressbar when showBar is true
	pb := progressbar.DefaultBytes(-1, "Downloading")
	for {
		result, err = down.Recv()
		if err != nil {
			if de, ok := err.(*dferrors.DfError); ok {
				logger.Errorf("dragonfly daemon returns error code %d/%s", de.Code, de.Message)
			} else {
				logger.Errorf("dragonfly daemon returns error %s", err)
			}
			break
		}
		if result.CompletedLength > 0 {
			pb.Set64(int64(result.CompletedLength))
		}
		if result.Done {
			pb.Describe("Downloaded")
			pb.Finish()
			end = time.Now()
			fmt.Printf("Task: %s\nPeer: %s\n", result.TaskId, result.PeerId)
			fmt.Printf("Download success, time cost: %dms, length: %d\n", end.Sub(start).Milliseconds(), result.CompletedLength)
			break
		}
	}
	if err != nil {
		logger.Errorf("download by dragonfly error: %s", err)
		return downloadFromSource(cfg, hdr)
	}
	return nil
}

func downloadFromSource(cfg *config.DfgetConfig, hdr map[string]string) (err error) {
	if cfg.DisableBackSource {
		err = fmt.Errorf("dfget download error, and back source disabled")
		logger.Warnf("%s", err)
		return err
	}
	fmt.Println("dfget download error, try to download from source")

	var (
		start    = time.Now()
		end      time.Time
		target   *os.File
		response io.ReadCloser
		written  int64
	)

	response, err = source.Download(context.Background(), cfg.URL, hdr)
	if err != nil {
		logger.Errorf("download from source error: %s", err)
		return err
	}
	defer response.Close()

	target, err = os.OpenFile(cfg.Output, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		logger.Errorf("open %s error: %s", cfg.Output, err)
		return err
	}

	written, err = io.Copy(target, response)
	if err != nil {
		logger.Errorf("copied %d bytes to %s, with error: %s", written, cfg.Output, err)
		return err
	}
	logger.Infof("copied %d bytes to %s", written, cfg.Output)
	end = time.Now()
	fmt.Printf("Download from source success, time cost: %dms\n", end.Sub(start).Milliseconds())

	// change permission
	logger.Infof("change own to uid %d gid %d", basic.UserID, basic.UserGroup)
	if err = os.Chown(cfg.Output, basic.UserID, basic.UserGroup); err != nil {
		logger.Errorf("change own failed: %s", err)
		return err
	}
	return nil
}

func parseHeader(s []string) map[string]string {
	hdr := map[string]string{}
	for _, h := range s {
		idx := strings.Index(h, ":")
		if idx > 0 {
			hdr[h[:idx]] = strings.TrimLeft(h[idx:], " ")
		}
	}
	return hdr
}
