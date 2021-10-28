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
	"fmt"
	"io"

	"d7y.io/dragonfly/v2/cdn/types"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"
	"github.com/pkg/errors"
)

const RangeHeaderName = "Range"

func (cm *Manager) download(ctx context.Context, task *types.SeedTask, detectResult *cacheResult) (io.ReadCloser, error) {
	headers := make(map[string]string)
	if detectResult.breakPoint > 0 {
		breakRange, err := rangeutils.GetBreakRange(detectResult.breakPoint, task.SourceFileLength)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to calculate the breakRange")
		}
		// check if Range in header? if Range already in Header, priority use this range
		if _, ok := headers[RangeHeaderName]; !ok {
			headers[RangeHeaderName] = fmt.Sprintf("bytes=%s", breakRange)
		}
	}
	task.Log().Infof("start download url %s at range: %d-%d: with header: %+v", task.URL, detectResult.breakPoint,
		task.SourceFileLength, task.Header)
	reader, responseHeader, err := source.DownloadWithResponseHeader(ctx, task.URL, headers)
	// update Expire info
	if err == nil {
		expireInfo := map[string]string{
			source.LastModified: responseHeader.Get(source.LastModified),
			source.ETag:         responseHeader.Get(source.ETag),
		}
		cm.updateExpireInfo(task.TaskID, expireInfo)
	}
	return reader, err
}
