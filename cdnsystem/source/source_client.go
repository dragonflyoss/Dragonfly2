package source

import (
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/pkg/stringutils"
	"github.com/go-openapi/strfmt"
	"strings"
)

var clients = make(map[string]ResourceClient)

func Register(schema string, resourceClient ResourceClient) {
	clients[schema] = resourceClient
}


type StatusCodeChecker func(int) bool

// SourceClient supply apis that interact with the source.
type ResourceClient interface {

	RegisterTLSConfig(rawURL string, insecure bool, caBlock []strfmt.Base64) error

	GetContentLength(url string, headers map[string]string) (int64, error)
	// checks whether the source supports breakpoint continuation
	IsSupportRange(url string, headers map[string]string) (bool, error)
	// checks if the cache is expired
	IsExpired(url string, headers , expireInfo map[string]string) (bool, error)
	// download from source
	Download(url string, headers map[string]string, checkCode StatusCodeChecker) (*types.DownloadResponse, error)
}

type ResourceClientAdaptor struct {
	clients map[string]ResourceClient
}

func (s *ResourceClientAdaptor) RegisterTLSConfig(url string, insecure bool, caBlock []strfmt.Base64) error {
	sourceClient, err := s.getSourceClient(url)
	if err != nil {
		return err
	}
	return sourceClient.RegisterTLSConfig(url, insecure, caBlock)
}

func (s *ResourceClientAdaptor) GetContentLength(url string, headers map[string]string) (int64, error) {
	sourceClient, err := s.getSourceClient(url)
	if err != nil {
		return -1, err
	}
	return sourceClient.GetContentLength(url, headers)
}

func (s *ResourceClientAdaptor) IsSupportRange(url string, headers map[string]string) (bool, error) {
	sourceClient, err := s.getSourceClient(url)
	if err != nil {
		return false, err
	}
	return sourceClient.IsSupportRange(url, headers)
}

func (s *ResourceClientAdaptor) IsExpired(url string, headers, expireInfo map[string]string) (bool, error) {
	sourceClient, err := s.getSourceClient(url)
	if err != nil {
		return false, err
	}
	return sourceClient.IsExpired(url, headers, expireInfo)
}

func (s *ResourceClientAdaptor) Download(url string, headers map[string]string, checkCode StatusCodeChecker) (*types.DownloadResponse, error) {
	sourceClient, err := s.getSourceClient(url)
	if err != nil {
		return nil, err
	}
	return sourceClient.Download(url, headers, checkCode)
}

func (s *ResourceClientAdaptor) getSchema(url string) (string, error) {
	if stringutils.IsEmptyStr(url) {
		return "", fmt.Errorf("url:%s is empty", url)
	}
	parts := strings.Split(url, ":")
	if len(parts) == 0 {
		return "", fmt.Errorf("cannot resolve url:%s schema", url)
	}
	return parts[0], nil
}

// Get a source client from manager with specified schema.
func (s *ResourceClientAdaptor) getSourceClient(url string) (ResourceClient, error) {
	schema, err := s.getSchema(url)
	if err != nil {
		return nil, err
	}
	client, ok := s.clients[schema]
	if !ok || client == nil {
		return nil, fmt.Errorf("not support schema %s client", schema)
	}
	return client, nil
}

func NewSourceClient() ResourceClient{
	return &ResourceClientAdaptor{
		clients: clients,
	}
}

