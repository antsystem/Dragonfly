package seed

import (
	"context"
	"sync"
	"time"

	"github.com/dragonflyoss/Dragonfly/pkg/queue"
	"github.com/dragonflyoss/Dragonfly/pkg/ratelimiter"
	"github.com/dragonflyoss/Dragonfly/supernode/httpclient"
)

var (
	localSeedManager SeedManager
	once             sync.Once
)

const (
	DefaultDownloadConcurrency = 4
	MaxDownloadConcurrency     = 8
	MinTotalLimit              = 2
	// 50MB
	DefaultBlockSize = 1024 * 1024 * 50

	defaultGcInterval = 2 * time.Minute
	defaultRetryTimes = 3

	defaultUploadRate = 10 * 1024 * 1024
)

// SeedManager is an interface which manages the seeds
type SeedManager interface {
	// Register a seed, manager will prefetch it to cache
	Register(key string, info *PreFetchInfo) (Seed, error)

	// UnRegister
	UnRegister(key string) error

	// SetPrefetchLimit limits the concurrency of downloading seed.
	// default is DefaultDownloadConcurrency.
	SetConcurrentLimit(limit int) (validLimit int)

	Get(key string) (Seed, error)

	List() ([]Seed, error)

	// stop the SeedManager
	Stop()
}

type seedManager struct {
	sync.Mutex

	// the context which is monitor by all loop
	ctx context.Context
	// call cancelFn to stop all loop
	cancelFn func()

	cacheDir        string
	concurrentLimit int
	totalLimit      int
	// when download seed file, it download by block which of size will set this.
	downloadBlockSize int64

	seedContainer map[string]Seed
	// lru queue which wide out the seed file, it is thread safe
	lru *queue.LRUQueue
	// the queue wait for prefetch, it is thread safe
	waitQueue queue.Queue

	// downloadCh notify the seed to prefetch
	downloadCh chan struct{}

	originClient httpclient.OriginHTTPClient

	uploadRate *ratelimiter.RateLimiter
}
