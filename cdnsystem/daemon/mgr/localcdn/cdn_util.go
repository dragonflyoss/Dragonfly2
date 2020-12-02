
package localcdn

import (
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"

	"github.com/dragonflyoss/Dragonfly2/pkg/timeutils"
)

var getCurrentTimeMillisFunc = timeutils.GetCurrentTimeMillis


func getUpdateTaskInfoWithStatusOnly(cdnStatus string) *types.CdnTaskInfo {
	return getUpdateTaskInfo(cdnStatus, "", 0)
}

func getUpdateTaskInfo(cdnStatus, realMD5 string, cdnFileLength int64) *types.CdnTaskInfo {
	return &types.CdnTaskInfo{
		CdnStatus:     cdnStatus,
		CdnFileLength: cdnFileLength,
		RealMd5:       realMD5,
	}
}
