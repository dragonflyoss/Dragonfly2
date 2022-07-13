package source

import "net/url"

// A URLEntry is an entry which read from url with specific protocol
// It is used in recursive downloading
type URLEntry interface {
	// URL uniquely define a UrlEntry
	URL() *url.URL

	// Name returns the name of the file (or subdirectory) described by the entry.
	// This name is only the final element of the path (the base name), not the entire path.
	// For example, Name would return "hello.go" not "home/gopher/hello.go".
	Name() string

	// IsDir reports whether the entry describes a directory.
	IsDir() bool
}

type urlEntry struct {
	url   *url.URL
	name  string
	isDir bool
}

func NewURLEntry(url *url.URL, name string, isDir bool) URLEntry {
	return &urlEntry{
		url:   url,
		name:  name,
		isDir: isDir,
	}
}

func (e *urlEntry) URL() *url.URL {
	return e.url
}

func (e *urlEntry) Name() string {
	return e.name
}

func (e *urlEntry) IsDir() bool {
	return e.isDir
}
