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
)

var (
	ErrShortRead = errors.New("short read")
)
