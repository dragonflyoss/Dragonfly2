package storage

type Storage interface {
	AddPeer()
	LoadPeers()
}

type storage struct {
	baseDir    string
	maxSize    uint
	maxBackups uint
}

// Option is a functional option for configuring the Storage
type Option func(s *storage)

// WithMaxSize set the maximum size in megabytes of storage file
func WithMaxSize(maxSize uint) Option {
	return func(s *storage) {
		s.maxSize = maxSize
	}
}

// WithMaxBackups set the maximum number of storage files to retain
func WithMaxBackups(maxBackups uint) Option {
	return func(s *storage) {
		s.maxBackups = maxBackups
	}
}

func New(baseDir string, options ...Option) Storage {
	s := &storage{
		baseDir: baseDir,
	}

	for _, opt := range options {
		opt(s)
	}

	return s
}
