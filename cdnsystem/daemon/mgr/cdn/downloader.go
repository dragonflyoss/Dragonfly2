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

package cdn

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/pkg/dferrors"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/httputils"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/rangeutils"
	"github.com/pkg/errors"
	"net/http"
)

// download downloads the file from the original address and
// sets the "Range" header to the unDownloaded file range.
//
// If the returned error is nil, the Response will contain a non-nil
// Body which the caller is expected to close.
func (cm *Manager) download(ctx context.Context, task *types.SeedTask, detectResult *detectCacheResult) (*types.DownloadResponse, error) {
	checkCode := []int{http.StatusOK, http.StatusPartialContent}
	headers := source.CopyHeader(nil, task.Headers)
	// cache partial data
	if detectResult.breakNum > 0 {
		breakRange, err := rangeutils.CalculateBreakRange(detectResult.breakNum, task.PieceSize, task.SourceFileLength)
		if err != nil {
			return nil, errors.Wrapf(dferrors.ErrInvalidValue, "failed to calculate the breakRange: %v", err)
		}
		// check if Range in header? if Range already in Header, use this range directly
		if !hasRange(headers) {
			headers["Range"] = httputils.ConstructRangeStr(breakRange)
		}
		checkCode = []int{http.StatusPartialContent}
	}

	logger.Infof("start to download for taskId(%s) with fileUrl: %s"+
		" header: %+v checkCode: %d", task.TaskID, task.Url, task.Headers, checkCode)
	return cm.resourceClient.Download(task.Url, headers, checkStatusCode(checkCode))
}

func hasRange(headers map[string]string) bool {
	if headers == nil {
		return false
	}
	_, ok := headers["Range"]
	return ok
}

func checkStatusCode(statusCode []int) func(int) bool {
	return func(status int) bool {
		for _, s := range statusCode {
			if status == s {
				return true
			}
		}
		return false
	}
}
