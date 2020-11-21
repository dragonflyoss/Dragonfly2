package sourceclient

import (
	"github.com/go-openapi/strfmt"
	"net/http"
)


type StatusCodeChecker func(int) bool

// SourceClient supply apis that interact with the source.
type SourceClient interface {

	RegisterTLSConfig(rawURL string, insecure bool, caBlock []strfmt.Base64)

	GetContentLength(url string, headers map[string]string) (int64, int, error)
	// checks whether the source supports breakpoint continuation
	IsSupportRange(url string, headers map[string]string) (bool, error)
	// checks if the cache is expired
	IsExpired(url string, headers map[string]string, lastModified int64, eTag string) (bool, error)
	// download from source
	Download(url string, headers map[string]string, checkCode StatusCodeChecker) (*http.Response, error)
}