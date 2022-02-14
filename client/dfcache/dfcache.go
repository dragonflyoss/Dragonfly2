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

package dfcache

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/pkg/errors"

	"d7y.io/dragonfly/v2/client/config"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/basic"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"d7y.io/dragonfly/v2/pkg/rpc/dfdaemon"
	daemonclient "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/client"
)

// Format that's used to cast the given cid to URI.
// "d7y" as scheme, and absolute path starts with "/"
const cidURIFormat = "d7y:/%s"

func newCid(cid string) string {
	return fmt.Sprintf(cidURIFormat, url.QueryEscape(cid))
}

func Stat(cfg *config.DfcacheConfig, client daemonclient.DaemonClient) error {
	var (
		ctx       = context.Background()
		cancel    context.CancelFunc
		statError error
	)

	if err := cfg.Validate(config.CmdStat); err != nil {
		return errors.Wrapf(err, "validate stat option failed")
	}

	wLog := logger.With("Cid", cfg.Cid, "Tag", cfg.Tag)
	wLog.Info("init success and start to stat")

	if cfg.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, cfg.Timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

	go func() {
		statError = statTask(ctx, client, cfg, wLog)
		cancel()
	}()

	<-ctx.Done()

	if ctx.Err() == context.DeadlineExceeded {
		return errors.Errorf("stat timeout(%s)", cfg.Timeout)
	}
	return statError
}

func statTask(ctx context.Context, client daemonclient.DaemonClient, cfg *config.DfcacheConfig, wLog *logger.SugaredLoggerOnWith) error {
	if client == nil {
		return errors.Errorf("stat has no daemon client")
	}

	start := time.Now()
	res, statError := client.StatTask(ctx, newStatRequest(cfg))
	if statError != nil {
		wLog.Errorf("daemon stat file error: %s", statError)
		return statError
	}
	if res.Code != base.Code_Success {
		return errors.Errorf("stat got code %s[%d]: %s", base.Code_name[int32(res.Code)], res.Code, res.Message)
	}

	wLog.Infof("task found in %.6f s", time.Since(start).Seconds())
	return nil
}

func newStatRequest(cfg *config.DfcacheConfig) *dfdaemon.StatTaskRequest {
	return &dfdaemon.StatTaskRequest{
		Cid: newCid(cfg.Cid),
		UrlMeta: &base.UrlMeta{
			Tag: cfg.Tag,
		},
		LocalOnly: cfg.LocalOnly,
	}
}

func Import(cfg *config.DfcacheConfig, client daemonclient.DaemonClient) error {
	var (
		ctx         = context.Background()
		cancel      context.CancelFunc
		importError error
	)

	if err := cfg.Validate(config.CmdImport); err != nil {
		return errors.Wrapf(err, "validate import option failed")
	}

	wLog := logger.With("Cid", cfg.Cid, "Tag", cfg.Tag, "file", cfg.Path)
	wLog.Info("init success and start to import")

	if cfg.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, cfg.Timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

	go func() {
		importError = importTask(ctx, client, cfg, wLog)
		cancel()
	}()

	<-ctx.Done()

	if ctx.Err() == context.DeadlineExceeded {
		return errors.Errorf("import timeout(%s)", cfg.Timeout)
	}
	return importError
}

func importTask(ctx context.Context, client daemonclient.DaemonClient, cfg *config.DfcacheConfig, wLog *logger.SugaredLoggerOnWith) error {
	if client == nil {
		return errors.Errorf("import has no daemon client")
	}

	start := time.Now()
	res, importError := client.ImportTask(ctx, newImportRequest(cfg))
	if importError != nil {
		wLog.Errorf("daemon import file error: %s", importError)
		return importError
	}
	if res.Code != base.Code_Success {
		return errors.Errorf("import got code %s[%d]: %s", base.Code_name[int32(res.Code)], res.Code, res.Message)
	}

	wLog.Infof("task imported successfully in %.6f s", time.Since(start).Seconds())
	return nil
}

func newImportRequest(cfg *config.DfcacheConfig) *dfdaemon.ImportTaskRequest {
	return &dfdaemon.ImportTaskRequest{
		Cid:  newCid(cfg.Cid),
		Path: cfg.Path,
		UrlMeta: &base.UrlMeta{
			Tag: cfg.Tag,
		},
	}
}

func Export(cfg *config.DfcacheConfig, client daemonclient.DaemonClient) error {
	var (
		ctx         = context.Background()
		cancel      context.CancelFunc
		exportError error
	)

	if err := cfg.Validate(config.CmdExport); err != nil {
		return errors.Wrapf(err, "validate export option failed")
	}

	wLog := logger.With("Cid", cfg.Cid, "Tag", cfg.Tag, "output", cfg.Output)
	wLog.Info("init success and start to export")

	if cfg.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, cfg.Timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

	go func() {
		exportError = exportTask(ctx, client, cfg, wLog)
		cancel()
	}()

	<-ctx.Done()

	if ctx.Err() == context.DeadlineExceeded {
		return errors.Errorf("export timeout(%s)", cfg.Timeout)
	}
	return exportError
}

func exportTask(ctx context.Context, client daemonclient.DaemonClient, cfg *config.DfcacheConfig, wLog *logger.SugaredLoggerOnWith) error {
	if client == nil {
		return errors.Errorf("export has no daemon client")
	}

	start := time.Now()
	res, exportError := client.ExportTask(ctx, newExportRequest(cfg))
	if exportError != nil {
		wLog.Errorf("daemon export file error: %s", exportError)
		return exportError
	}
	if res.Code != base.Code_Success {
		return errors.Errorf("export got code %s[%d]: %s", base.Code_name[int32(res.Code)], res.Code, res.Message)
	}

	wLog.Infof("task exported successfully in %.6f s", time.Since(start).Seconds())
	return exportError
}

func newExportRequest(cfg *config.DfcacheConfig) *dfdaemon.ExportTaskRequest {
	return &dfdaemon.ExportTaskRequest{
		Cid:     newCid(cfg.Cid),
		Output:  cfg.Output,
		Timeout: uint64(cfg.Timeout),
		Limit:   float64(cfg.RateLimit),
		UrlMeta: &base.UrlMeta{
			Tag: cfg.Tag,
		},
		Uid:       int64(basic.UserID),
		Gid:       int64(basic.UserGroup),
		LocalOnly: cfg.LocalOnly,
	}
}

func Delete(cfg *config.DfcacheConfig, client daemonclient.DaemonClient) error {
	var (
		ctx         = context.Background()
		cancel      context.CancelFunc
		deleteError error
	)

	if err := cfg.Validate(config.CmdDelete); err != nil {
		return errors.Wrapf(err, "validate delete option failed")
	}

	wLog := logger.With("Cid", cfg.Cid, "Tag", cfg.Tag)
	wLog.Info("init success and start to delete")

	if cfg.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, cfg.Timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

	go func() {
		deleteError = deleteTask(ctx, client, cfg, wLog)
		cancel()
	}()

	<-ctx.Done()

	if ctx.Err() == context.DeadlineExceeded {
		return errors.Errorf("delete timeout(%s)", cfg.Timeout)
	}
	return deleteError
}

func deleteTask(ctx context.Context, client daemonclient.DaemonClient, cfg *config.DfcacheConfig, wLog *logger.SugaredLoggerOnWith) error {
	if client == nil {
		return errors.Errorf("delete has no daemon client")
	}

	start := time.Now()
	res, deleteError := client.DeleteTask(ctx, newDeleteRequest(cfg))
	if deleteError != nil {
		wLog.Errorf("daemon delete file error: %s", deleteError)
		return deleteError
	}
	if res.Code != base.Code_Success {
		return errors.Errorf("delete got code %s[%d]: %s", base.Code_name[int32(res.Code)], res.Code, res.Message)
	}

	wLog.Infof("task deleted successfully in %.6f s", time.Since(start).Seconds())
	return nil
}

func newDeleteRequest(cfg *config.DfcacheConfig) *dfdaemon.DeleteTaskRequest {
	return &dfdaemon.DeleteTaskRequest{
		Cid: newCid(cfg.Cid),
		UrlMeta: &base.UrlMeta{
			Tag: cfg.Tag,
		},
	}
}
