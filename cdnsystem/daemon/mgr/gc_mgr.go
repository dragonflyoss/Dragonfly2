package mgr

import "context"

// GCMgr as an interface defines all operations about gc operation.
type GCMgr interface {

	// StartGC starts to execute GC with a new goroutine.
	StartGC(ctx context.Context)

	// GCTask is used to do the gc task job with specified taskID.
	// The CDN file will be deleted when the full is true.
	GCTask(ctx context.Context, taskID string, full bool)
}
