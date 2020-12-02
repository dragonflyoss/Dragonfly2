package mgr

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/config"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/store"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/pkg/ratelimiter"
	"github.com/prometheus/client_golang/prometheus"
)

type CDNBuilder func(cfg *config.Config, cacheStore *store.Store,
	resourceClient source.ResourceClient, rateLimiter *ratelimiter.RateLimiter, register prometheus.Registerer) (CDNMgr, error)

var cdnBuilderMap = make(map[config.CDNPattern]CDNBuilder)

func Register(name config.CDNPattern, builder CDNBuilder) {
	cdnBuilderMap[name] = builder
}

// get an implementation of the interface of CDNMgr
func GetCDNManager(cfg *config.Config, storeMgr *store.Manager, resourceClient source.ResourceClient, rateLimiter *ratelimiter.RateLimiter,
	register prometheus.Registerer) (CDNMgr, error) {
	cdnPattern := cfg.CDNPattern
	if cdnPattern == "" {
		cdnPattern = config.CDNPatternLocal
	}

	cdnBuilder, ok := cdnBuilderMap[cdnPattern]
	if !ok {
		return nil, fmt.Errorf("unexpected cdn pattern(%s) which must be in [\"local\", \"source\"]", cdnPattern)
	}
	storageDriver := cfg.StorageDriver
	if storageDriver == "" {
		storageDriver = config.LocalStorageDriver
	}
	storeLocal, err := storeMgr.Get(storageDriver)
	if err != nil {
		return nil, err
	}
	return cdnBuilder(cfg, storeLocal, resourceClient, rateLimiter, register)
}

// CDNMgr as an interface defines all operations against CDN and
// operates on the underlying files stored on the local disk, etc.
type CDNMgr interface {
	// TriggerCDN will trigger CDN to download the file from sourceUrl.
	// It includes the following steps:
	// 1). download the source file
	// 2). write the file to disk
	//
	// In fact, it's a very time consuming operation.
	// So if not necessary, it should usually be executed concurrently.
	// In addition, it's not thread-safe.
	TriggerCDN(ctx context.Context, taskInfo *types.SeedTaskInfo) (*types.SeedTaskInfo, error)

	// GetHTTPPath returns the http download path of taskID.
	GetHTTPPath(ctx context.Context, taskInfo *types.SeedTaskInfo) (path string, err error)

	// GetStatus gets the status of the file.
	GetStatus(ctx context.Context, taskID string) (cdnStatus string, err error)

	// GetGCTaskIDs returns the taskIDs that should exec GC operations as a string slice.
	//
	// It should return nil when the free disk of cdn storage is lager than config.YoungGCThreshold.
	// It should return all taskIDs that are not running when the free disk of cdn storage is less than config.FullGCThreshold.
	GetGCTaskIDs(ctx context.Context, taskMgr SeedTaskMgr) ([]string, error)

	// CheckFile checks the file whether exists.
	CheckFileExist(ctx context.Context, taskID string) bool

	// Delete the cdn meta with specified taskID.
	// The file on the disk will be deleted when the force is true.
	Delete(ctx context.Context, taskID string, force bool) error

}
