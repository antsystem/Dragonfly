package seed

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly/dfget/config"
	dfdaemonCfg "github.com/dragonflyoss/Dragonfly/dfdaemon/config"
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"github.com/dragonflyoss/Dragonfly/pkg/netutils"
	"github.com/dragonflyoss/Dragonfly/pkg/queue"
	"github.com/dragonflyoss/Dragonfly/supernode/httpclient"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/pkg/errors"
)

var(
	localSeedManager SeedManager
	once 		sync.Once
)

const(
	DefaultDownloadConcurrency = 4
	MaxDownloadConcurrency = 8
	MinTotalLimit = 2
	// 50MB
	DefaultBlockSize = 1024 * 1024 * 50

	defaultGcInterval = 2 * time.Minute
	defaultRetryTimes = 3
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

func NewSeedManager(metaDir string) SeedManager {
	once.Do(func() {
		var err error
		// todo: config the total limit
		localSeedManager, err = newSeedManager(filepath.Join(metaDir, "seed"), 0, 50, 0)
		if err != nil {
			panic(err)
		}
	})

	return localSeedManager
}

// concurrentLimit limits the concurrency of downloading seed.
// totalLimit limits the total of seed file.
func newSeedManager(cacheDir string, concurrentLimit int, totalLimit int, downloadBlock int64) (SeedManager, error) {
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

	// mkdir meta dir
	err := os.MkdirAll(filepath.Join(cacheDir, "meta"), 0774)
	if err != nil {
		return nil, err
	}

	// mkdir content dir
	err = os.MkdirAll(filepath.Join(cacheDir, "content"), 0774)
	if err != nil {
		return nil, err
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
		originClient: httpclient.NewOriginClient(),
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

	originClient    httpclient.OriginHTTPClient
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
	sr.Lock()
	defer sr.Unlock()

	delete(sr.seedContainer, taskID)
	sr.lru.Delete(taskID)

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
	sr.Lock()
	defer sr.Unlock()

	ret := make([]Seed, len(sr.seedContainer))
	i := 0
	for _, s := range sr.seedContainer {
		ret[i] = s
		i ++
	}

	return ret, nil
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
	sr.lru.Delete(key)
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

// gcLoop run the loop to
func (sr *seedManager) gcLoop(ctx context.Context) {
	ticker := time.NewTicker(defaultGcInterval)
	defer ticker.Stop()

	for{
		select {
		case <- ctx.Done():
			logrus.Infof("context done, return gcLoop")

		case <- ticker.C:
			logrus.Infof("start to  gc loop")
			sr.gcExpiredSeed()
		}
	}
}

func (sr *seedManager) gcExpiredSeed() {
	list, err := sr.List()
	if err != nil {
		return
	}

	for _, ob := range list {
		sd, ok := ob.(*seed)
		if !ok {
			continue
		}

		if sd.verifyExpired() {
			sr.gcSeed(sd.TaskId, sd)
		}
	}
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
		err   error
	)

	if sd.getHttpFileLength() == 0 {
		header := map[string]string{}
		for k, v := range sd.Header {
			header[k] = v[0]
		}

		length, err := sr.getHTTPFileLength(sd.TaskId, sd.Url, header)
		if  err != nil {
			// todo: handle the error

		}

		sd.setHttpFileLength(length)
	}

	dn := newLocalDownloader(sd.Url, sd.Header, sd.rate)
	// download 50MB per request
	downloadBlock := sr.downloadBlockSize
	defaultTimeout := netutils.CalculateTimeout(int64(downloadBlock), 0, config.DefaultMinRate, 10*time.Second)
	retryTimeout := defaultTimeout * 2
	retry := 0

	start = 0
	for{
		end = start + downloadBlock - 1
		if end >= sd.HttpFileLength - 1 {
			end = sd.HttpFileLength - 1
		}

		_, err = sd.cache.Seek(start, io.SeekStart)
		if err != nil {
			//todo: handle the error
			goto handleErr
		}

		timeout := defaultTimeout
		if retry > 0 {
			timeout = retryTimeout
		}

		length, err := dn.Download(ctx, httputils.RangeStruct{StartIndex: start, EndIndex: end}, timeout, sd.cache)
		if err != nil {
			// todo: handle the error
			code, _, ok := isHttpError(err)
			if ok {
				if code != http.StatusRequestTimeout &&  code != http.StatusTooManyRequests &&
					code != http.StatusResetContent {
					goto handleErr
				}
			}else if !isTimeoutError(err) {
				goto handleErr
			}

			// retry
			if retry > defaultRetryTimes {
				logrus.Errorf("retry %d times, failed to download: %v", retry, err)
				goto handleErr
			}

			retry ++
			continue
		}

		retry = 0

		sd.cache.Sync()
		err = sd.updateSize(start + length)
		if err != nil {
			// todo: handle the error
		}

		if end == sd.HttpFileLength - 1 {
			// download finished
			goto finished
		}

		start = end + 1
	}

handleErr:
	sd.cache.Close()
	sd.setStatus(INITIAL_STATUS)
	go func() {
		sr.downloadCh <- struct{}{}
	}()
	ch <- PreFetchResult{Success: false, Canceled: true, Err: err}
	// todo:
	return

finished:
	sd.cache.Close()
	sd.setStatus(FINISHED_STATUS)
	sd.RefreshExpiredTime(0)
	go func() {
		sr.downloadCh <- struct{}{}
	}()
	ch <- PreFetchResult{Success: true, Canceled: false}
}


func (sr *seedManager) getHTTPFileLength(taskID, url string, headers map[string]string) (int64, error) {
	fileLength, code, err := sr.originClient.GetContentLength(url, headers)
	if err != nil {
		return -1, errors.Wrapf(errortypes.ErrUnknownError, "failed to get http file Length: %v", err)
	}

	if code == http.StatusUnauthorized || code == http.StatusProxyAuthRequired {
		return -1, errors.Wrapf(errortypes.ErrAuthenticationRequired, "taskID: %s,code: %d", taskID, code)
	}
	if code != http.StatusOK && code != http.StatusPartialContent {
		logrus.Warnf("failed to get http file length with unexpected code: %d", code)
		if code == http.StatusNotFound {
			return -1, errors.Wrapf(errortypes.ErrURLNotReachable, "taskID: %s, url: %s", taskID, url)
		}
		return -1, nil
	}

	return fileLength, nil
}

