package source

import (
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/go-openapi/strfmt"
)

type StatusCodeChecker func(int) bool

// SourceClient supply apis that interact with the source.
type SourceClient interface {

	RegisterTLSConfig(rawURL string, insecure bool, caBlock []strfmt.Base64)

	GetContentLength(url string, headers map[string]string) (int64, error)
	// checks whether the source supports breakpoint continuation
	IsSupportRange(url string, headers map[string]string) (bool, error)
	// checks if the cache is expired
	IsExpired(url string, headers , expireInfo map[string]string) (bool, error)
	// download from source
	Download(url string, headers map[string]string) (*types.DownloadResponse, error)
}

// Client is a wrapper of the source client which implements the interface of SourceClient.
type Client struct {
	// schema is a protocol of source, example http/https/oss/hdfs
	schema string
	// config is used to init source client.
	config interface{}
	// sourceClient implements the interface of source client.
	sourceClient SourceClient
}

func (c *Client) GetContentLength(url string, headers map[string]string) (int64, error) {
	return c.sourceClient.GetContentLength(url, headers)
}

func (c *Client) IsSupportRange(url string, headers map[string]string) (bool, error) {
	return c.sourceClient.IsSupportRange(url, headers)
}

func (c *Client) IsExpired(url string, headers, expireInfo map[string]string) (bool, error) {
	return c.sourceClient.IsExpired(url, headers, expireInfo)
}

func (c *Client) Download(url string, headers map[string]string) (*types.DownloadResponse, error) {
	return c.sourceClient.Download(url, headers)
}


// NewClient creates a new client instance.
func NewClient(schema string, builder SourceClientBuilder, cfg string) (*Client, error) {
	if schema == "" || builder == nil {
		return nil, fmt.Errorf("plugin name or builder cannot be nil")
	}

	// init driver with specific config
	driver, err := builder(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to init source client schema: %s, err: %v", schema, err)
	}

	return &Client{
		schema:       schema,
		config:       cfg,
		sourceClient: driver,
	}, nil
}

// Type returns the plugin type: SourceClientPlugin.
func (c *Client) Type() config.PluginType {
	return config.SourceClientPlugin
}

// Name returns the plugin name.
func (c *Client) Name() string {
	return c.schema
}


