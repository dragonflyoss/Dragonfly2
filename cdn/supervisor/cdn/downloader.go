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

	"github.com/pkg/errors"

	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
)

func (cm *manager) download(ctx context.Context, seedTask *task.SeedTask, breakPoint int64) (io.ReadCloser, error) {
	var err error
	breakRange := seedTask.Range
	if breakPoint > 0 {
		// todo replace task.SourceFileLength with totalSourceFileLength to get BreakRange
		breakRange, err = getBreakRange(breakPoint, seedTask.Range, seedTask.SourceFileLength)
		if err != nil {
			return nil, errors.Wrapf(err, "calculate the breakRange")
		}
	}
	seedTask.Log().Infof("start downloading URL %s at range %s with header %s", seedTask.RawURL, breakRange, seedTask.Header)
	downloadRequest, err := source.NewRequestWithContext(ctx, seedTask.RawURL, seedTask.Header)
	if err != nil {
		return nil, errors.Wrap(err, "create download request")
	}
	if !stringutils.IsBlank(breakRange) {
		downloadRequest.Header.Add(source.Range, breakRange)
	}
	body, expireInfo, err := source.DownloadWithExpireInfo(downloadRequest)
	// update Expire info
	if err == nil {
		cm.updateExpireInfo(seedTask.ID, map[string]string{
			source.LastModified: expireInfo.LastModified,
			source.ETag:         expireInfo.ETag,
		})
	}
	return body, err
}

func getBreakRange(breakPoint int64, taskRange string, fileTotalLength int64) (string, error) {
	if breakPoint <= 0 {
		return "", errors.Errorf("breakPoint is non-positive: %d", breakPoint)
	}
	if fileTotalLength <= 0 {
		return "", errors.Errorf("file length is non-positive: %d", fileTotalLength)
	}
	if stringutils.IsBlank(taskRange) {
		return fmt.Sprintf("%d-%d", breakPoint, fileTotalLength-1), nil
	}
	requestRange, err := rangeutils.ParseRange(taskRange, uint64(fileTotalLength))
	if err != nil {
		return "", errors.Errorf("parse range failed, taskRange: %s, fileTotalLength: %d: %v", taskRange, fileTotalLength, err)
	}
	if breakPoint >= int64(requestRange.EndIndex-requestRange.StartIndex+1) {
		return "", errors.Errorf("breakPoint %d is larger than or equal with length of download required %s", breakPoint, requestRange)
	}
	return fmt.Sprintf("%d-%d", requestRange.StartIndex+uint64(breakPoint), requestRange.EndIndex), nil
}
