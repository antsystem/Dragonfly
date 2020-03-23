package seed

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
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
	GetStatus() string
	TaskID() string
	URL() string
	Length() (int64, error)
}

// seed represents a seed which could be downloaded by other peers
type seed struct {
	sync.RWMutex

	Header      map[string][]string `json:"header"`
	Url         string              `json:"url"`
	// current content size, it may be not full downloaded
	Size        int64               `json:"size"`
	ContentPath string              `json:"contentPath"`
	TaskId      string              `json:"TaskId"`

	Status string 			   `json:"Status"`

	sm      *seedManager	   `json:"-"`
	cache	cacheBuffer

	rate          *ratelimiter.RateLimiter
	ExpireTimeDur time.Duration 	`json:"ExpireTimeDur"`
	ExpireTime    time.Time			`json:"expireTime"`

	metaPath      string
	metaBakPath   string

	expireCh      chan struct{}
}

func newSeed(sm *seedManager, taskID string, info *PreFetchInfo) (Seed, error) {
	contentPath := sm.seedContentPath(taskID)

	cache, _, err :=  newFileCacheBuffer(contentPath, 0, false)
	if err != nil {
		return nil, err
	}

	return &seed{
		Status: INITIAL_STATUS,
		Url:    info.URL,
		Header: info.Header,
		Size:   info.Length,
		TaskId: taskID,
		cache:  cache,
		sm:     sm,
		metaPath: sm.seedMetaPath(taskID),
		metaBakPath: sm.seedMetaBakPath(taskID),
		ContentPath: contentPath,
		expireCh:  make(chan struct{}),
	}, nil
}

// restore from meta, if it is not finished, return nil.
func restoreFromMeta(sm *seedManager, taskID string, data []byte) (Seed, error) {
	sd := &seed{}
	err := json.Unmarshal(data, &sd)
	if err != nil {
		return nil, err
	}

	if sd.Status != FINISHED_STATUS {
		return nil, fmt.Errorf("status not %s", FINISHED_STATUS)
	}

	sd.sm = sm
	sd.TaskId = taskID
	cache, exist, err :=  newFileCacheBuffer(sd.ContentPath, sd.Size, sd.Status == FINISHED_STATUS)
	if err != nil {
		return nil, err
	}

	// if not exist, return
	if ! exist {
		sd.cache.Remove()
		return nil, fmt.Errorf("status not %s", FINISHED_STATUS)
	}

	sd.cache = cache
	sd.metaBakPath = sm.seedMetaBakPath(taskID)
	sd.metaPath = sm.seedMetaPath(taskID)

	return sd, nil
}

func (sd *seed) Prefetch(limiter *ratelimiter.RateLimiter, expireTime time.Duration) (finishCh <- chan PreFetchResult, err error) {
	ch := make(chan PreFetchResult, 1)
	sd.RLock()
	defer sd.RUnlock()

	if sd.Status == FINISHED_STATUS || sd.Status == FETCHING_STATUS {
		ch <- PreFetchResult{Canceled: true}
		return ch, nil
	}

	sd.Status = FETCHING_STATUS
	sd.rate = limiter
	sd.ExpireTimeDur = expireTime

	go func() {
		//todo: try to
		sd.sm.addToDownloadQueue(prefetchSt{sd: sd, ch: ch})
	}()

	return ch, nil
}

func (sd *seed) Delete() error {
	sd.Lock()
	defer sd.Unlock()

	sd.Status = INITIAL_STATUS

	// remove the meta data
	os.Remove(sd.metaPath)
	os.Remove(sd.metaBakPath)

	// remove the cache
	return sd.cache.Remove()
}

func (sd *seed) RefreshExpiredTime(expiredTime time.Duration) {
	sd.Lock()
	defer sd.Unlock()

	sd.refreshExpiredTimeWithOutLock(expiredTime)
}

func (sd *seed) refreshExpiredTimeWithOutLock(expiredTime time.Duration) {
	sd.ExpireTimeDur = expiredTime
	sd.ExpireTime = time.Now().Add(expiredTime)
	sd.sm.updateLRU(sd)
}

func (sd *seed) Download(off int64, size int64) (io.ReadCloser, error) {
	sd.RLock()
	defer sd.RUnlock()

	if sd.Status != FINISHED_STATUS {
		return nil, fmt.Errorf("not finished")
	}

	// refresh expire time
	sd.refreshExpiredTimeWithOutLock(sd.ExpireTimeDur)

	return sd.cache.ReadStream(off, size)
}

func (sd *seed) NotifyExpired() (chan <- struct{}, error) {
	return sd.expireCh, nil
}

func (sd *seed) GetStatus() string {
	sd.RLock()
	defer sd.RUnlock()

	return sd.Status
}

func (sd *seed) TaskID() string {
	sd.RLock()
	defer sd.RUnlock()

	return sd.TaskId
}

func (sd *seed) URL() string {
	sd.RLock()
	defer sd.RUnlock()

	return sd.Url
}

func (sd *seed) Length() (int64, error) {
	sd.RLock()
	defer sd.RUnlock()

	if sd.Status != FINISHED_STATUS {
		return 0, errors.New("")
	}

	return sd.Size, nil
}

func (sd *seed) setStatus(status string) error {
	sd.Lock()
	defer sd.Unlock()

	sd.Status = status
	return sd.storeWithoutLock()
}

// update the size of
func (sd *seed) updateSize(size int64) error {
	sd.Lock()
	defer sd.Unlock()

	sd.Size = size
	return sd.storeWithoutLock()
}

// store the meta data to local fs
func (sd *seed) storeWithoutLock() error {
	data, err := json.Marshal(sd)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(sd.metaBakPath, data, 0644)
	if err != nil {
		return err
	}

	return os.Rename(sd.metaBakPath, sd.metaPath)
}
