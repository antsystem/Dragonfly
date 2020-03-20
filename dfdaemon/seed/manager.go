package seed

import (
	"github.com/dragonflyoss/Dragonfly/pkg/queue"
	"sync"
)

const(
	DefaultDownloadConcurrency = 3
)

// SeedManager is an interface which manages the seeds
type SeedManager interface {
	// Register a seed, manager will prefetch it to cache
	Register(taskID string, info *PreFetchInfo) (Seed, error)

	// UnRegister
	UnRegister(taskID string) error

	// SetPrefetchLimit limits the concurrency of downloading seed.
	// default is DefaultDownloadConcurrency.
	SetConcurrentLimit(limit int)  (validLimit int)

	Get(taskID string) (Seed, error)

	List() ([]Seed, error)
}

// concurrentLimit limits the concurrency of downloading seed.
// totalLimit limits the total of seed file.
func NewSeedManager(cacheDir string, concurrentLimit int, totalLimit int) (SeedManager, error) {
	return &seedManager{}, nil
}

type seedManager struct {
	sync.Mutex

	cacheDir    string
	concurrentLimit int
	totalLimit  int

	seedContainer map[string]Seed
	lru			*queue.LRUQueue

}

func (sr *seedManager) Register(taskID string, info *PreFetchInfo) (Seed, error) {
	return nil, nil
}

// UnRegister
func (sr *seedManager) UnRegister(taskID string) error {
	return nil
}

// SetPrefetchLimit limit the concurrency of downloading seed.
// default is DefaultDownloadConcurrency.
func (sr *seedManager) SetConcurrentLimit(limit int)  (validLimit int) {
	return 0
}

func (sr *seedManager) Get(taskID string) (Seed, error) {
	return nil, nil
}

func (sr *seedManager) List() ([]Seed, error) {
	return nil, nil
}
