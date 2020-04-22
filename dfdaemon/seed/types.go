package seed

import (
	"github.com/dragonflyoss/Dragonfly/pkg/ratelimiter"
	"time"
)

type PreFetchInfo struct {
	TaskID string
	URL    string
	Header map[string][]string
	FullLength int64
	BlockOrder uint32

	ExpireTimeDur  time.Duration
}

type PreFetchResult struct {
	Success bool

	Err error
	// if canceled, caller need not to do other
	Canceled bool
}

type DownloadStatus struct {
	Finished bool
	Canceled bool
	Start    int64
	Length   int64
}

type SeedBaseOpt struct {
	BaseDir string
	Info    PreFetchInfo

	downPreFunc func(sd Seed)
}

type RateOpt struct {
	DownloadRateLimiter *ratelimiter.RateLimiter
}

type NewSeedManagerOpt struct {
	StoreDir string
	ConcurrentLimit int
	TotalLimit int
	DownloadBlockOrder uint32
	OpenMemoryCache bool

	// if download rate < 0, means no rate limit; else default limit
	DownloadRate int64
	UploadRate   int64

	// water level which is used to expire the seed
	// if HighLevel is reached, start to prepare the expire
	HighLevel    uint

	// expire will be stopped util water level is smaller than LowLevel.
	LowLevel     uint
}
