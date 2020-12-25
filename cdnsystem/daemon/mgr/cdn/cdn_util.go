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
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"

	"github.com/dragonflyoss/Dragonfly2/pkg/util/timeutils"
)

var getCurrentTimeMillisFunc = timeutils.GetCurrentTimeMillis

func getUpdateTaskInfoWithStatusOnly(cdnStatus string) *types.SeedTask {
	return getUpdateTaskInfo(cdnStatus, "", 0)
}

func getUpdateTaskInfo(cdnStatus, realMD5 string, cdnFileLength int64) *types.SeedTask {
	return &types.SeedTask{
		CdnStatus:     cdnStatus,
		SourceRealMd5: realMD5,
		CdnFileLength: cdnFileLength,
	}
}
