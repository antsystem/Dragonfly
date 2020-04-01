package seed

import (
	"io"
	"sync"
	"time"

	"github.com/dragonflyoss/Dragonfly/pkg/ratelimiter"
)

const (
	FINISHED_STATUS = "finished"
	FETCHING_STATUS = "fetching"
	INITIAL_STATUS  = "initial"
	DEAD_STATUS     = "dead"

	defaultTimeLayout = time.RFC3339Nano
)

type Seed interface {
	// Prefetch will start to download to local cache.
	// expiredTime starts from time when already downloaded.
	Prefetch(limiter *ratelimiter.RateLimiter, expiredTime time.Duration) (finishCh <-chan PreFetchResult, err error)

	// Delete will delete the local cache and unregister from SeedManager
	Delete() error

	// RefreshExpiredTime will refresh the expired time from now
	RefreshExpiredTime(expiredTime time.Duration)

	// Download providers the range download, if seed not include the range,
	// directSource decide to whether to download from source.
	Download(start int64, length int64, directSource bool) (io.ReadCloser, error)

	// NotifyExpired
	NotifyExpired() (<-chan struct{}, error)

	GetHttpFileLength() int64
	SetHttpFileLength(int64)
	GetStatus() string
	TaskID() string
	Key() string
	URL() string
	Headers() map[string][]string
	CurrentSize() int64
}

// seed represents a seed which could be downloaded by other peers
type seed struct {
	sync.RWMutex

	Header map[string][]string `json:"header"`
	Url    string              `json:"url"`
	// current content size, it may be not full downloaded
	Size           int64  `json:"size"`
	ContentPath    string `json:"contentPath"`
	SeedKey        string `json:"seedKey"`
	HttpFileLength int64  `json:"httpFileLength"`
	TaskId         string `json:"taskId"`

	Status string `json:"Status"`

	sm    *seedManager `json:"-"`
	cache cacheBuffer

	rate          *ratelimiter.RateLimiter
	uploadRate    *ratelimiter.RateLimiter
	ExpireTimeDur time.Duration `json:"expireTimeDur"`
	expireTime    time.Time     `json:"-"`
	DeadLineTime  string        `json:"deadLineTime"`

	metaPath    string
	metaBakPath string

	expireCh chan struct{}
}
