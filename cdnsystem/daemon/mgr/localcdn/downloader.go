package localcdn

import (
	"context"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"net/http"

	errorType "github.com/dragonflyoss/Dragonfly2/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly2/pkg/httputils"
	"github.com/dragonflyoss/Dragonfly2/pkg/rangeutils"
)

// download downloads the file from the original address and
// sets the "Range" header to the undownloaded file range.
//
// If the returned error is nil, the Response will contain a non-nil
// Body which the caller is expected to close.
func (cm *Manager) download(ctx context.Context,  taskID, url string, headers map[string]string,
	startPieceNum int32, sourceFileLength int64, pieceSize int32) (*types.DownloadResponse, error) {
	checkCode := []int{http.StatusOK, http.StatusPartialContent}
	if startPieceNum > 0 {
		breakRange, err := rangeutils.CalculateBreakRange(startPieceNum, pieceSize, sourceFileLength)
		if err != nil {
			return nil, errors.Wrapf(errorType.ErrInvalidValue, "failed to calculate the breakRange: %v", err)
		}
		// check if Range in header? if Range already in Header, use this range directly
		if !hasRange(headers) {
			headers = source.CopyHeader(
				map[string]string{"Range": httputils.ConstructRangeStr(breakRange)},
				headers)
		}
		checkCode = []int{http.StatusPartialContent}
	}

	logrus.Infof("start to download for taskId(%s) with fileUrl: %s"+
		" header: %v checkCode: %d", taskID, url, headers, checkCode)
	return cm.resourceClient.Download(url, headers, checkStatusCode(checkCode))
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
