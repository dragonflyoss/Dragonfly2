
package localcdn

import (
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"

	"github.com/dragonflyoss/Dragonfly2/pkg/util/timeutils"
)

var getCurrentTimeMillisFunc = timeutils.GetCurrentTimeMillis

func getUpdateTaskInfoWithStatusOnly(cdnStatus string) *types.SeedTaskInfo {
	return getUpdateTaskInfo(cdnStatus, "", 0)
}

func getUpdateTaskInfo(cdnStatus, realMD5 string, cdnFileLength int64) *types.SeedTaskInfo {
	return &types.SeedTaskInfo{
		CdnStatus:     cdnStatus,
		SourceRealMd5: realMD5,
		CdnFileLength: cdnFileLength,
	}
}
