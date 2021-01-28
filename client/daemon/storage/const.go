package storage

import "os"

const (
	taskData     = "data"
	taskMetaData = "metadata"

	defaultFileMode      = os.FileMode(0644)
	defaultDirectoryMode = os.FileMode(0755)

	SimpleLocalTaskStoreStrategy  = StoreStrategy("io.d7y.storage.v2.simple")
	AdvanceLocalTaskStoreStrategy = StoreStrategy("io.d7y.storage.v2.advance")
)
