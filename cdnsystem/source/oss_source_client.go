package source

import (
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/go-openapi/strfmt"
)

const ossClient = "oss"

func init() {
	sourceClient, err := newOSSSourceClient()
	if err != nil {

	}
	Register(ossClient, sourceClient)
}

// NewHttpSourceClient returns a new HttpSourceClient.

// httpSourceClient is an implementation of the interface of SourceClient.
type ossSourceClient struct {
}

func (o ossSourceClient) RegisterTLSConfig(rawURL string, insecure bool, caBlock []strfmt.Base64) error {
	panic("implement me")
}

func (o ossSourceClient) GetContentLength(url string, headers map[string]string) (int64, error) {
	panic("implement me")
}

func (o ossSourceClient) IsSupportRange(url string, headers map[string]string) (bool, error) {
	panic("implement me")
}

func (o ossSourceClient) IsExpired(url string, headers, expireInfo map[string]string) (bool, error) {
	panic("implement me")
}

func (o ossSourceClient) Download(url string, headers map[string]string, checkCode StatusCodeChecker) (*types.DownloadResponse, error) {
	panic("implement me")
}

func newOSSSourceClient() (ResourceClient, error) {
	return nil, nil
}
