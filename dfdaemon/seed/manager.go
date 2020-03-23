package seed

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"github.com/dragonflyoss/Dragonfly/pkg/queue"
	"path/filepath"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const(
	DefaultDownloadConcurrency = 4
	MaxDownloadConcurrency = 8
	MinTotalLimit = 2
	// 50MB
	DefaultBlockSize = 1024 * 1024 * 50
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
func NewSeedManager(cacheDir string, concurrentLimit int, totalLimit int, downloadBlock int64) (SeedManager, error) {
	if concurrentLimit > MaxDownloadConcurrency {
		concurrentLimit = MaxDownloadConcurrency
	}

	if concurrentLimit <= 0 {
		concurrentLimit = DefaultDownloadConcurrency
	}

	if totalLimit < MinTotalLimit {
		totalLimit = MinTotalLimit
	}

	if downloadBlock == 0 {
		downloadBlock = DefaultBlockSize
	}

	downloadCh := make(chan struct{}, concurrentLimit)
	for i:= 0; i < concurrentLimit; i ++ {
		downloadCh <- struct{}{}
	}

	sr := &seedManager{
		cacheDir: cacheDir,
		concurrentLimit: concurrentLimit,
		totalLimit: totalLimit,
		seedContainer: make(map[string]Seed),
		lru: queue.NewLRUQueue(totalLimit),
		waitQueue: queue.NewQueue(0),
		downloadCh: downloadCh,
		downloadBlockSize: downloadBlock,
	}

	go sr.prefetchLoop(context.Background())
	go sr.gcLoop(context.Background())

	return sr, nil
}

type seedManager struct {
	sync.Mutex

	cacheDir    	string
	concurrentLimit int
	totalLimit  	int
	// when download seed file, it download by block which of size will set this.
	downloadBlockSize int64

	seedContainer 	map[string]Seed
	// lru queue which wide out the seed file, it is thread safe
	lru			  	*queue.LRUQueue
	// the queue wait for prefetch, it is thread safe
	waitQueue		queue.Queue

	// downloadCh notify the seed to prefetch
	downloadCh		chan struct{}
}

func (sr *seedManager) Register(taskID string, info *PreFetchInfo) (Seed, error) {
	sr.Lock()
	defer sr.Unlock()

	_, exist := sr.seedContainer[taskID]
	if exist {
		return nil, fmt.Errorf("Confilct ")
	}

	sd, err := newSeed(sr, taskID, info)
	if err != nil {
		return nil, err
	}
	sr.seedContainer[taskID] = sd

	return sd, nil
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
	sr.Lock()
	defer sr.Unlock()

	sd, exist := sr.seedContainer[taskID]
	if exist {
		return sd, nil
	}

	return nil, fmt.Errorf("data not found")
}

func (sr *seedManager) List() ([]Seed, error) {
	return nil, nil
}

func (sr *seedManager) seedMetaPath(taskID string) string {
	return filepath.Join(sr.cacheDir, "meta", fmt.Sprintf("%s.meta", taskID))
}

func (sr *seedManager) seedMetaBakPath(taskID string) string {
	return filepath.Join(sr.cacheDir, "meta", fmt.Sprintf("%s.meta.bak", taskID))
}

func (sr *seedManager) seedContentPath(taskID string) string {
	return filepath.Join(sr.cacheDir, "content", fmt.Sprintf("%s.service", taskID))
}

func (sr *seedManager) updateLRU(sd *seed) {
	obsoleteKey, obsoleteData := sr.lru.Put(sd.TaskId, sd)
	if obsoleteKey != "" {
		go sr.gcSeed(obsoleteKey, obsoleteData.(*seed))
	}
}

func (sr *seedManager) gcSeed(key string, sd *seed) {
	logrus.Infof("gc seed TaskId  %s, Url %s", sd.TaskId, sd.Url)
	// delete from map
	sr.Lock()
	delete(sr.seedContainer, key)
	sr.Unlock()

	// remove sd
	sd.Delete()
}

// prefetchLoop handle the seed file
func (sr *seedManager) prefetchLoop(ctx context.Context) {
	for{
		ob, exist := sr.waitQueue.PollTimeout(2 * time.Second)
		if ! exist {
			continue
		}

		st, ok := ob.(prefetchSt)
		if !ok {
			continue
		}

		select {
			case <- ctx.Done():
				return
			case <- sr.downloadCh:
				break
		}

		go sr.downloadSeed(ctx, st.sd, st.ch)
	}
}

func (sr *seedManager) gcLoop(ctx context.Context) {

}

func (sr *seedManager) addToDownloadQueue(st prefetchSt) {
	sr.waitQueue.Put(st)
}

// downloadSeed download the seed file, if success or failed, it will send to sr.downloadCh to notify
// the next seed to download.
func (sr *seedManager) downloadSeed(ctx context.Context, sd *seed, ch chan PreFetchResult) {
	var(
		start int64
		end   int64
	)

	dn := newLocalDownloader(sd.Url, sd.Header, sd.rate)
	// download 50MB per request
	downloadBlock := sr.downloadBlockSize

	start = 0
	for{
		end = start + downloadBlock - 1
		length, err := dn.Download(ctx, httputils.RangeStruct{StartIndex: start, EndIndex: end}, sd.cache)
		if err != nil {
			// todo: handle the error
		}

		sd.cache.Sync()
		err = sd.updateSize(start + length)
		if err != nil {
			// todo: handle the error
		}

		if length < downloadBlock {
			// download finished
			goto finished
		}

		start = end + 1
	}

//caceled:
//	sd.cache.Close()
finished:
	sd.cache.Close()
	sd.setStatus(FINISHED_STATUS)
	go func() {
		sr.downloadCh <- struct{}{}
	}()
	ch <- PreFetchResult{Success: true, Canceled: false}
}


