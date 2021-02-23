package storage

import (
	"os"

	"github.com/pkg/errors"
)

const (
	taskData     = "data"
	taskMetaData = "metadata"

	defaultFileMode      = os.FileMode(0644)
	defaultDirectoryMode = os.FileMode(0755)

	SimpleLocalTaskStoreStrategy  = StoreStrategy("io.d7y.storage.v2.simple")
	AdvanceLocalTaskStoreStrategy = StoreStrategy("io.d7y.storage.v2.advance")
)

var (
	ErrShortRead = errors.New("short read")
)
