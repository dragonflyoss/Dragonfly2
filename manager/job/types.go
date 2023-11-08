package job

import (
	"errors"
	"fmt"
)

// preheatImage is image information for preheat.
type preheatImage struct {
	protocol string
	domain   string
	name     string
	tag      string
}

func (p *preheatImage) manifestURL() string {
	return fmt.Sprintf("%s://%s/v2/%s/manifests/%s", p.protocol, p.domain, p.name, p.tag)
}

func (p *preheatImage) blobsURL(digest string) string {
	return fmt.Sprintf("%s://%s/v2/%s/blobs/%s", p.protocol, p.domain, p.name, digest)
}

// parseManifestURL parses manifest url.
func parseManifestURL(url string) (*preheatImage, error) {
	r := accessURLPattern.FindStringSubmatch(url)
	if len(r) != 5 {
		return nil, errors.New("parse access url failed")
	}

	return &preheatImage{
		protocol: r[1],
		domain:   r[2],
		name:     r[3],
		tag:      r[4],
	}, nil
}
