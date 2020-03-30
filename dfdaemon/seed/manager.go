package seed

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dragonflyoss/Dragonfly/dfget/config"
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"github.com/dragonflyoss/Dragonfly/pkg/netutils"
	"github.com/dragonflyoss/Dragonfly/pkg/queue"
	"github.com/dragonflyoss/Dragonfly/supernode/httpclient"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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

func NewSeedManager(metaDir string, totalLimitOfSeeds int) SeedManager {
	once.Do(func() {
		var err error
		// todo: config the total limit
		localSeedManager, err = newSeedManager(filepath.Join(metaDir, "seed"), 0, totalLimitOfSeeds, 0)
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
	for i := 0; i < concurrentLimit; i++ {
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

	ctx, cancelFn := context.WithCancel(context.Background())

	sr := &seedManager{
		ctx:               ctx,
		cancelFn:          cancelFn,
		cacheDir:          cacheDir,
		concurrentLimit:   concurrentLimit,
		totalLimit:        totalLimit,
		seedContainer:     make(map[string]Seed),
		lru:               queue.NewLRUQueue(totalLimit),
		waitQueue:         queue.NewQueue(0),
		downloadCh:        downloadCh,
		downloadBlockSize: downloadBlock,
		originClient:      httpclient.NewOriginClient(),
	}

	sr.restore(ctx)

	go sr.prefetchLoop(ctx)
	go sr.gcLoop(ctx)

	return sr, nil
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
}

func (sr *seedManager) Register(key string, info *PreFetchInfo) (Seed, error) {
	sr.Lock()
	defer sr.Unlock()

	_, exist := sr.seedContainer[key]
	if exist {
		return nil, fmt.Errorf("confilct key %s", key)
	}

	sd, err := newSeed(sr, key, info)
	if err != nil {
		return nil, err
	}
	sr.seedContainer[key] = sd

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
func (sr *seedManager) SetConcurrentLimit(limit int) (validLimit int) {
	return 0
}

func (sr *seedManager) Get(key string) (Seed, error) {
	sr.Lock()
	defer sr.Unlock()

	sd, exist := sr.seedContainer[key]
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
		i++
	}

	return ret, nil
}

func (sr *seedManager) Stop() {
	sr.cancelFn()
}

func (sr *seedManager) restore(ctx context.Context) {
	metaDir := filepath.Join(sr.cacheDir, "meta")
	fis, err := ioutil.ReadDir(metaDir)
	if err != nil {
		logrus.Errorf("failed to read cache dir %s: %v", metaDir, err)
		return
	}

	seeds := []*seed{}

	// todo: clear the unexpected meta file
	for _, fi := range fis {
		if fi.IsDir() {
			continue
		}

		name := fi.Name()
		if !strings.HasSuffix(name, ".meta") {
			continue
		}

		key := strings.TrimSuffix(name, ".meta")
		data, err := ioutil.ReadFile(filepath.Join(metaDir, name))
		if err != nil {
			logrus.Errorf("failed to read file %s: %v", name, err)
			continue
		}

		sed, err := restoreFromMeta(sr, key, data)
		if err != nil {
			logrus.Errorf("failed to restore from meta %s: %v", key, err)
			continue
		}

		sd := sed.(*seed)
		if sd.isExpired() {
			// if expired, remove the seed
			sd.Delete()
			continue
		}

		seeds = append(seeds, sd)
	}

	sort.Slice(seeds, func(i, j int) bool {
		return seeds[i].expireTime.Before(seeds[j].expireTime)
	})

	// add seed to map
	for _, sd := range seeds {
		sr.seedContainer[sd.Key()] = sd
	}

	// update seed to url queue
	for _, sd := range seeds {
		sr.updateLRU(sd)
	}
}

func (sr *seedManager) seedMetaPath(key string) string {
	return filepath.Join(sr.cacheDir, "meta", fmt.Sprintf("%s.meta", key))
}

func (sr *seedManager) seedMetaBakPath(key string) string {
	return filepath.Join(sr.cacheDir, "meta", fmt.Sprintf("%s.meta.bak", key))
}

func (sr *seedManager) seedContentPath(key string) string {
	return filepath.Join(sr.cacheDir, "content", fmt.Sprintf("%s.service", key))
}

func (sr *seedManager) updateLRU(sd *seed) {
	obsoleteKey, obsoleteData := sr.lru.Put(sd.SeedKey, sd)
	if obsoleteKey != "" {
		go sr.gcSeed(obsoleteKey, obsoleteData.(*seed))
	}
}

func (sr *seedManager) gcSeed(key string, sd *seed) {
	logrus.Infof("gc seed SeedKey  %s, Url %s", sd.SeedKey, sd.Url)
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
	for {
		ob, exist := sr.waitQueue.PollTimeout(2 * time.Second)
		if !exist {
			continue
		}

		st, ok := ob.(prefetchSt)
		if !ok {
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-sr.downloadCh:
			break
		}

		go sr.downloadSeed(ctx, st.sd, st.ch)
	}
}

// gcLoop run the loop to
func (sr *seedManager) gcLoop(ctx context.Context) {
	ticker := time.NewTicker(defaultGcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logrus.Infof("context done, return gcLoop")
			return

		case <-ticker.C:
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

		if sd.isExpired() {
			sr.gcSeed(sd.SeedKey, sd)
		}
	}
}

func (sr *seedManager) addToDownloadQueue(st prefetchSt) {
	sr.waitQueue.Put(st)
}

// downloadSeed download the seed file, if success or failed, it will send to sr.downloadCh to notify
// the next seed to download.
func (sr *seedManager) downloadSeed(ctx context.Context, sd *seed, ch chan PreFetchResult) {
	var (
		start int64
		end   int64
		err   error
	)

	dn := newLocalDownloader(sd.Url, sd.Header, sd.rate)
	// download 50MB per request
	downloadBlock := sr.downloadBlockSize
	defaultTimeout := netutils.CalculateTimeout(int64(downloadBlock), 0, config.DefaultMinRate, 10 * time.Second)
	retryTimeout := defaultTimeout * 2
	retry := 0
	downloaderLength := int64(0)
	httpFileLength := int64(0)

	if sd.GetHttpFileLength() == 0 {
		header := map[string]string{}
		for k, v := range sd.Header {
			header[k] = v[0]
		}

		length, err := sr.getHTTPFileLength(sd.SeedKey, sd.Url, header)
		if err != nil {
			goto handleErr
		}

		sd.SetHttpFileLength(length)
	}

	start = sd.CurrentSize()
	httpFileLength = sd.GetHttpFileLength()

	for {
		end = start + downloadBlock - 1
		if end > httpFileLength - 1 {
			end = httpFileLength - 1
		}

		timeout := defaultTimeout
		if retry > 0 {
			timeout = retryTimeout
		}

		downloaderLength, err = dn.DownloadToWriterAt(ctx, httputils.RangeStruct{StartIndex: start, EndIndex: end}, timeout, start, sd.cache)
		if err != nil {
			code, _, ok := isHttpError(err)
			if ok {
				if code != http.StatusRequestTimeout && code != http.StatusTooManyRequests &&
					code != http.StatusResetContent {
					goto handleErr
				}
			} else if !isTimeoutError(err) {
				goto handleErr
			}

			// retry
			if retry > defaultRetryTimes {
				logrus.Errorf("retry %d times, failed to download: %v", retry, err)
				goto handleErr
			}

			retry++
			continue
		}

		retry = 0

		sd.cache.Sync()
		err = sd.updateSize(start + downloaderLength)
		if err != nil {
			goto handleErr
		}
		sd.cache.LockSize(sd.cache.Size())

		if end == httpFileLength - 1 {
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

	ch <- NewPreFetchResult(false, true, fmt.Errorf("download failed: %v", err), func() {
		close(ch)
	})

	return

finished:
	sd.cache.Close()
	sd.setStatus(FINISHED_STATUS)
	sd.RefreshExpiredTime(0)
	go func() {
		sr.downloadCh <- struct{}{}
	}()
	ch <- NewPreFetchResult(true, false, nil, func() {
		close(ch)
	})
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
