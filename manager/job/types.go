package job

import distribution "github.com/distribution/distribution/v3"

type invalidManifestFormatError struct{}

func (invalidManifestFormatError) Error() string {
	return "unsupported manifest format"
}

type manifestReference interface {
	References() []distribution.Descriptor
}
