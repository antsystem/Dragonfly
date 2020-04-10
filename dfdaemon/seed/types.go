package seed

import (
	"github.com/dragonflyoss/Dragonfly/pkg/ratelimiter"
)

type PreFetchInfo struct {
	TaskID string
	URL    string
	Header map[string][]string
	FullLength int64
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

type prefetchSt struct {
	sd *seed
	ch chan PreFetchResult
}

type SeedBaseOpt struct {
	MetaDir		string
	BlockOrder  uint32
	Info        *PreFetchInfo
}

type RateOpt struct {
	DownloadRateLimiter *ratelimiter.RateLimiter
}
