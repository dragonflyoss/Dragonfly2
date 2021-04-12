package storage

import (
	"os"

	"github.com/pkg/errors"
)

const (
	taskData     = "data"
	taskMetaData = "metadata"

	defaultFileMode      = os.FileMode(4644)
	defaultDirectoryMode = os.FileMode(4755)
)

var (
	ErrShortRead = errors.New("short read")
)
