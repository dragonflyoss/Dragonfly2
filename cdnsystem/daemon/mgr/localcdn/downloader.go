package localcdn

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"net/http"

	errorType "github.com/dragonflyoss/Dragonfly2/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/httputils"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/rangeutils"
)

// download downloads the file from the original address and
// sets the "Range" header to the undownloaded file range.
//
// If the returned error is nil, the Response will contain a non-nil
// Body which the caller is expected to close.
func (cm *Manager) download(ctx context.Context,  task *types.SeedTaskInfo,
	detectResult *detectCacheResult) (*types.DownloadResponse, error) {
	checkCode := []int{http.StatusOK, http.StatusPartialContent}
	headers := source.CopyHeader(nil, task.Headers)
	// cache partial data
	if detectResult.breakNum > 0 {
		breakRange, err := rangeutils.CalculateBreakRange(detectResult.breakNum, task.PieceSize, task.SourceFileLength)
		if err != nil {
			return nil, errors.Wrapf(errorType.ErrInvalidValue, "failed to calculate the breakRange: %v", err)
		}
		// check if Range in header? if Range already in Header, use this range directly
		if !hasRange(headers) {
			headers["Range"] = httputils.ConstructRangeStr(breakRange)
		}
		checkCode = []int{http.StatusPartialContent}
	}

	logrus.Infof("start to download for taskId(%s) with fileUrl: %s"+
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
