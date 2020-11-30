package types

import "io"

type DownloadResponse struct {
	Body       io.ReadCloser
	ExpireInfo map[string]string
}
