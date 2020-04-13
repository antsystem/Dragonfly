package seed

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dragonflyoss/Dragonfly/pkg/queue"
	"github.com/dragonflyoss/Dragonfly/pkg/ratelimiter"
	"github.com/dragonflyoss/Dragonfly/supernode/httpclient"

	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
)

var (
	localSeedManager SeedManager
	once             sync.Once
)

const (
	DefaultDownloadConcurrency = 4
	MaxDownloadConcurrency     = 8
	MinTotalLimit              = 2

	defaultGcInterval = 2 * time.Minute
	defaultRetryTimes = 3

	defaultUploadRate = 10 * 1024 * 1024


	defaultTimeLayout = time.RFC3339Nano

	// 128KB
	defaultBlockOrder = 17
)

// SeedManager is an interface which manages the seeds
type SeedManager interface {
	// Register a seed, manager will prefetch it to cache
	Register(key string, info *PreFetchInfo) (Seed, error)

	// UnRegister
	UnRegister(key string) error

	// refresh expire time of seed
	RefreshExpireTime(key string, expireTimeDur time.Duration) error

	// NotifyExpired
	NotifyExpired(key string) (<-chan struct{}, error)

	// Prefetch will add seed to the prefetch list by key, and then prefetch by the concurrent limit.
	Prefetch(key string, perDownloadSize int64) (<- chan struct{}, error)

	// SetPrefetchLimit limits the concurrency of downloading seed.
	// default is DefaultDownloadConcurrency.
	SetConcurrentLimit(limit int) (validLimit int)

	Get(key string) (Seed, error)

	List() ([]Seed, error)

	// stop the SeedManager
	Stop()
}

// seedWrapObj wraps the seed and other info
type seedWrapObj struct {
	sync.RWMutex
	sd     		Seed
	// if seed prefetch
	prefetchCh 	chan struct{}
	// if seed is expired, the expiredCh will be closed.
	expiredCh   chan struct{}

	Key             string
	ExpireTimeDur   time.Duration
	ExpireTime      time.Time
	PerDownloadSize int64

	// if prefetch has been called, set prefetch to true to prevent other goroutine prefetch again.
	prefetch        bool
}

func (sw *seedWrapObj) isExpired() bool {
	sw.RLock()
	defer sw.RUnlock()

	// if expire time dur is 0, never expired.
	if sw.ExpireTimeDur == 0 {
		return false
	}

	return sw.ExpireTime.Before(time.Now())
}

func (sw *seedWrapObj) refreshExpireTime(expireTimeDur time.Duration) error {
	sw.Lock()
	defer sw.Unlock()

	if expireTimeDur != 0 {
		sw.ExpireTimeDur = expireTimeDur
	}

	sw.ExpireTime = time.Now().Add(sw.ExpireTimeDur)
	//todo: store them to local fs

	return nil
}

func (sw *seedWrapObj) release() {
	sw.Lock()
	defer sw.Unlock()

	close(sw.expiredCh)
	sw.sd.Stop()
	sw.sd.Delete()
}

type seedManager struct {
	sync.Mutex

	// the context which is monitor by all loop
	ctx context.Context
	// call cancelFn to stop all loop
	cancelFn func()

	storeDir        string
	concurrentLimit int
	totalLimit      int

	seedContainer map[string]*seedWrapObj
	// lru queue which wide out the seed file, it is thread safe
	lru *queue.LRUQueue
	// the queue wait for prefetch, it is thread safe
	waitQueue queue.Queue

	// downloadCh notify the seed to prefetch
	downloadCh chan struct{}

	originClient httpclient.OriginHTTPClient

	uploadRate *ratelimiter.RateLimiter

	defaultBlockOrder  uint32
	openMemoryCache    bool
}

func NewSeedManager(storeDir string, concurrentLimit int, totalLimit int, downloadBlockOrder uint32) (SeedManager, error) {



}

func newSeedManager(storeDir string, concurrentLimit int, totalLimit int, downloadBlockOrder uint32, openMemoryCache bool) (SeedManager, error) {
	if concurrentLimit > MaxDownloadConcurrency {
		concurrentLimit = MaxDownloadConcurrency
	}

	if concurrentLimit <= 0 {
		concurrentLimit = DefaultDownloadConcurrency
	}

	if totalLimit < MinTotalLimit {
		totalLimit = MinTotalLimit
	}

	if downloadBlockOrder == 0 {
		downloadBlockOrder = defaultBlockOrder
	}

	if downloadBlockOrder < 10 || downloadBlockOrder > 31 {
		return nil, fmt.Errorf("downloadBlockOrder should be in range[10, 31]")
	}

	downloadCh := make(chan struct{}, concurrentLimit)
	for i := 0; i < concurrentLimit; i++ {
		downloadCh <- struct{}{}
	}

	// mkdir store dir
	err := os.MkdirAll(storeDir, 0774)
	if err != nil {
		return nil, err
	}

	ctx, cancelFn := context.WithCancel(context.Background())

	sm := &seedManager{
		ctx:               ctx,
		cancelFn:          cancelFn,
		storeDir:          storeDir,
		concurrentLimit:   concurrentLimit,
		totalLimit:        totalLimit,
		seedContainer:     make(map[string]*seedWrapObj),
		lru:               queue.NewLRUQueue(totalLimit),
		waitQueue:         queue.NewQueue(0),
		downloadCh:        downloadCh,
		originClient:      httpclient.NewOriginClient(),
		uploadRate: 	   ratelimiter.NewRateLimiter(defaultUploadRate/100, 100),
		defaultBlockOrder: downloadBlockOrder,
		openMemoryCache:   openMemoryCache,
	}

	sm.restore(ctx)

	go sm.prefetchLoop(ctx)
	go sm.gcLoop(ctx)

	return sm, nil
}

func (sm *seedManager) Register(key string, info *PreFetchInfo) (Seed, error) {
	sm.Lock()
	defer sm.Unlock()

	obj, ok := sm.seedContainer[key]
	if ok {
		return obj.sd, errortypes.ErrTaskIDDuplicate
	}

	if info.BlockOrder == 0 {
		info.BlockOrder = sm.defaultBlockOrder
	}

	opt := SeedBaseOpt{
		MetaDir:  filepath.Join(sm.storeDir, "seed",  key),
		BlockOrder: info.BlockOrder,
		Info: info,
	}

	sd, err := NewSeed(opt, RateOpt{}, sm.openMemoryCache)
	if err != nil {
		return nil, err
	}

	sm.seedContainer[key] = &seedWrapObj{
		sd:  sd,
		prefetchCh: make(chan struct{}),
		expiredCh:  make(chan struct{}),
	}

	return sd, nil
}

func (sm *seedManager) UnRegister(key string) error {
	sw, err := sm.getSeedWrapObj(key)
	if err != nil {
		return err
	}

	sm.gcSeed(key, sw.sd)
	return nil
}

func (sm *seedManager) RefreshExpireTime(key string, expireTimeDur time.Duration) error {
	sw, err := sm.getSeedWrapObj(key)
	if err != nil {
		return err
	}

	err = sw.refreshExpireTime(expireTimeDur)
	if err != nil {
		return err
	}

	sm.updateLRU(key, sw.sd)
	return nil
}

func (sm *seedManager) NotifyExpired(key string) (<-chan struct{}, error) {
	sw, err := sm.getSeedWrapObj(key)
	if  err != nil {
		return nil, err
	}

	return sw.expiredCh, nil
}

func (sm *seedManager) List() ([]Seed, error) {
	sm.Lock()
	defer sm.Unlock()

	ret := make([]Seed, len(sm.seedContainer))
	i := 0
	for _, obj := range sm.seedContainer {
		ret[i] = obj.sd
		i++
	}

	return ret, nil
}

func (sm *seedManager) Prefetch(key string, perDownloadSize int64) (<- chan struct{}, error) {
	sw, err := sm.getSeedWrapObj(key)
	if err != nil {
		return nil, err
	}

	sw.Lock()
	if !sw.prefetch {
		sw.prefetch = true
		sw.PerDownloadSize = perDownloadSize
		sw.prefetchCh = make(chan struct{})

		// add seed to waitQueue, it will be polled out by handles goroutine to start to prefetch
		sm.waitQueue.Put(sw)
	}
	sw.Unlock()
	return sw.prefetchCh, nil
}

func (sm *seedManager) Get(key string) (Seed, error) {
	obj, err := sm.getSeedWrapObj(key)
	if err != nil {
		return nil, err
	}

	return obj.sd, nil
}

func (sm *seedManager) SetConcurrentLimit(limit int) (validLimit int) {
	return 0
}

func (sm *seedManager) Stop() {
	if sm.cancelFn != nil {
		sm.cancelFn()
	}
}

func (sm *seedManager) restore(ctx context.Context) {
	metaDir := filepath.Join(sm.storeDir, "seed")
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

func (sm *seedManager) listSeedWrapObj() []*seedWrapObj {
	sm.Lock()
	defer sm.Unlock()

	ret := make([]*seedWrapObj, len(sm.seedContainer))
	i := 0
	for _, obj := range sm.seedContainer {
		ret[i] = obj
		i++
	}

	return ret
}

func (sm *seedManager) getSeedWrapObj(key string) (*seedWrapObj, error) {
	sm.Lock()
	defer sm.Unlock()

	obj, ok := sm.seedContainer[key]
	if !ok {
		return obj, errortypes.ErrDataNotFound
	}

	return obj, nil
}

func (sm *seedManager) gcSeed(key string, sd Seed) {
	logrus.Infof("gc seed SeedKey  %s, Url %s", key, sd.URL())
	sw, err := sm.getSeedWrapObj(key)
	if err != nil {
		return
	}

	// delete from map
	sm.Lock()
	delete(sm.seedContainer, key)
	sm.lru.Delete(key)
	sm.Unlock()

	sw.release()
}

// prefetchLoop poll the seeds from waitQueue and start to prefetch them.
func (sm *seedManager) prefetchLoop(ctx context.Context) {
	for {
		ob, exist := sm.waitQueue.PollTimeout(2 * time.Second)
		if !exist {
			continue
		}

		sw, ok := ob.(*seedWrapObj)
		if !ok {
			continue
		}

		select {
		case <-ctx.Done():
			return
		// 	downloadCh will control the limit of concurrent prefetch.
		case <-sm.downloadCh:
			break
		}

		go sm.downloadSeed(ctx, sw)
	}
}

func (sm *seedManager) downloadSeed(ctx context.Context, sw *seedWrapObj) {
	sw.RLock()
	perDownloadSize := sw.PerDownloadSize
	sw.RUnlock()

	waitPrefetchCh, err := sw.sd.Prefetch(perDownloadSize)
	if err != nil {
		//todo: error handle
		goto errHandle
	}

	select
	{
		case <- ctx.Done():
			// todo:
		case <- waitPrefetchCh:
			break
	}

	sw.Lock()
	close(sw.prefetchCh)
	sw.Unlock()

	// notify the prefetchLoop to prefetch next seed.
	sm.downloadCh <- struct{}{}

	result, err := sw.sd.GetPrefetchResult()
	if err != nil {
		return
	}

	// if prefetch success, add seed to lru queue.
	if result.Success {
		sm.updateLRU(sw.Key, sw.sd)
	}

	return

errHandle:

}

func (sm *seedManager) updateLRU(key string, sd Seed) {
	obsoleteKey, obsoleteData := sm.lru.Put(key, sd)
	if obsoleteKey != "" {
		go sm.gcSeed(obsoleteKey, obsoleteData.(*seed))
	}
}

// gcLoop run the loop to gc the seed file.
func (sm *seedManager) gcLoop(ctx context.Context) {
	ticker := time.NewTicker(defaultGcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logrus.Infof("context done, return gcLoop")
			return

		case <-ticker.C:
			logrus.Infof("start to  gc loop")
			sm.gcExpiredSeed()
		}
	}
}

func (sm *seedManager) gcExpiredSeed() {
	list := sm.listSeedWrapObj()
	for _, sw := range list {
		if sw.isExpired() {
			sm.gcSeed(sw.Key, sw.sd)
		}
	}
}

func (sm *seedManager) seedWrapMetaPath() {

}
