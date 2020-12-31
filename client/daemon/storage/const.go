package storage

import "os"

const (
	taskData     = "data"
	taskMetaData = "metadata"

	defaultFileMode      = os.FileMode(0644)
	defaultDirectoryMode = os.FileMode(0755)

	SimpleLocalTaskStoreDriver  = Driver("io.d7y.storage.v2.simple")
	AdvanceLocalTaskStoreDriver = Driver("io.d7y.storage.v2.advance")
)
