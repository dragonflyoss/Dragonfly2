package job

import (
	"fmt"

	"github.com/containerd/containerd/platforms"
	specs "github.com/opencontainers/image-spec/specs-go/v1"
)

// preheatImage is image information for preheat.
type preheatImage struct {
	protocol string
	domain   string
	name     string
	tag      string
}

func (p *preheatImage) buildManifestURL(digest string) string {
	return fmt.Sprintf("%s://%s/v2/%s/manifests/%s", p.protocol, p.domain, p.name, digest)
}

func (p *preheatImage) buildBlobsURL(digest string) string {
	return fmt.Sprintf("%s://%s/v2/%s/blobs/%s", p.protocol, p.domain, p.name, digest)
}

// invalidManifestFormatError is error for invalid manifest format.
type invalidManifestFormatError struct{}

func (invalidManifestFormatError) Error() string {
	return "unsupported manifest format"
}

// noMatchesErr is error for no matching manifest.
type noMatchesErr struct {
	platform specs.Platform
}

func (e noMatchesErr) Error() string {
	return fmt.Sprintf("no matching manifest for %s in the manifest list entries", formatPlatform(e.platform))
}

func formatPlatform(platform specs.Platform) string {
	if platform.OS == "" {
		platform = platforms.DefaultSpec()
	}
	return platforms.Format(platform)
}
