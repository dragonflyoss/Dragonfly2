package source

import (
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/pkg/stringutils"
	"strings"
	"sync"

	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/plugins"
)

//SourceClientBuilder is a function that creates a new source client plugin instant
//with the giving conf.
type SourceClientBuilder func(conf string) (SourceClient, error)

// Register defines an interface to register a source client builder with specified name.
// All source clients should call this function to register itself to the sourceClientFactory.
func Register(name string, builder SourceClientBuilder) {
	var buildFun plugins.Builder = func(conf string) (plugin plugins.Plugin, e error) {
		return NewClient(name, builder, conf)
	}
	plugins.RegisterPlugin(config.StoragePlugin, name, buildFun)
}

// Manager manages clients.
type Manager struct {
	cfg           *config.Config
	defaultClient *Client
	mutex         sync.Mutex
}

// NewManager creates a source client manager.
func NewManager(cfg *config.Config) (*Manager, error) {
	return &Manager{
		cfg: cfg,
	}, nil
}


func (sm *Manager) GetSchema(url string)(string, error)  {
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
func (sm *Manager) Get(url string) (*Client, error) {
	name, err := sm.GetSchema(url)
	if err != nil {
		return nil, err
	}
	v := plugins.GetPlugin(config.SourceClientPlugin, name)
	if v == nil {
		return nil, fmt.Errorf("not existed client: %s", name)
	}
	if client, ok := v.(*Client); ok {
		return client, nil
	}
	return nil, fmt.Errorf("get client error: unknown reason")
}
