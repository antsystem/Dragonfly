package seed

import (
	"errors"
	"io"
	"sync"
	"time"

	"github.com/dragonflyoss/Dragonfly/pkg/ratelimiter"
)

const(
	FINISHED_STATUS = "finished"
	FETCHING_STATUS = "fetching"
	INITIAL_STATUS = "initial"
)

type Seed interface {
	// Prefetch will start to download to local cache.
	// expiredTime starts from time when already downloaded.
	Prefetch(limiter *ratelimiter.RateLimiter, expiredTime time.Duration) (finishCh <- chan PreFetchResult, err error)

	// Delete will delete the local cache and unregister from SeedManager
	Delete() error

	// RefreshExpiredTime will refresh the expired time from now
	RefreshExpiredTime(expiredTime time.Duration)

	// Download providers the range download.
	Download(start int64, length int64) (io.ReadCloser, error)

	// NotifyExpired
	NotifyExpired() (chan <- struct{}, error)

	//

	Status() string
	TaskID() string
	URL() string
	Length() (int64, error)
}

// seed represents a seed which could be downloaded by other peers
type seed struct {
	sync.RWMutex

	header  map[string][]string
	url		string
	length  int64
	path	string
	taskID  string

	status  string

	sm      *seedManager
}

func newSeed() Seed {
	return &seed{}
}

func (sd *seed) Prefetch(limiter *ratelimiter.RateLimiter, expiredTime time.Duration) (finishCh <- chan PreFetchResult, err error) {
	finishCh = make(chan PreFetchResult, 1)
	sd.RLock()
	defer sd.RUnlock()

	if sd.status == FINISHED_STATUS {

	}

	go func() {

	}()

	return finishCh, nil
}

func (sd *seed) Delete() error {

}

func (sd *seed) RefreshExpiredTime(expiredTime time.Duration) {

}

func (sd *seed) Download(start int64, length int64) (io.ReadCloser, error) {

}

func (sd *seed) NotifyExpired() (chan <- struct{}, error) {

}

func (sd *seed) Status() string {
	sd.RLock()
	defer sd.RUnlock()

	return sd.status
}

func (sd *seed) TaskID() string {
	sd.RLock()
	defer sd.RUnlock()

	return sd.taskID
}

func (sd *seed) URL() string {
	sd.RLock()
	defer sd.RUnlock()

	return sd.url
}

func (sd *seed) Length() (int64, error) {
	sd.RLock()
	defer sd.RUnlock()

	if sd.status != FINISHED_STATUS {
		return 0, errors.New("")
	}

	return sd.length, nil
}
