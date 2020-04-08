package seed

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly/dfget/config"
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"github.com/dragonflyoss/Dragonfly/pkg/netutils"
	"github.com/sirupsen/logrus"
	"io"
	"net/http"
	"os"
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

	// 16KB
	defaultBlockOrder = 14
)

type Seed interface {
	// Prefetch will start to download to local cache.
	Prefetch(perDownloadSize int64) (<- chan struct{}, error)

	// GetPrefetchResult should be called after notify by prefetch chan.
	GetPrefetchResult() (PreFetchResult, error)

	// Delete will delete the local cache and release the resource.
	Delete() error

	// Download providers the range download, if seed not include the range,
	// directSource decide to whether to download from source.
	Download(off int64, size int64) (io.ReadCloser, error)

	GetFullSize() int64
	GetStatus() string
	TaskID() string
	URL() string
	Headers() map[string][]string
}

// seed represents a seed which could be downloaded by other peers
type seed struct {
	sync.RWMutex

	Header		 	map[string][]string `json:"header"`
	Url    			string              `json:"url"`
	// current content size, it may be not full downloaded
	ContentPath 	string `json:"contentPath"`
	FullSize    	int64  `json:"fullSize"`
	TaskId      	string `json:"taskId"`

	Status string 			`json:"Status"`
	// blockOrder should be [10,31], it means the order of block size.
	BlockOrder		uint32		`json:"blockOrder"`
	BlockMetaBits   []byte		`json:"blockMetaBits"`

	cache 			cacheBuffer

	rate          	*ratelimiter.RateLimiter
	uploadRate    	*ratelimiter.RateLimiter

	metaPath    	string
	metaBakPath 	string

	down    	    downloader
	// blockMeta bitmap may should be sync to fs.
	blockMeta		*bitmap
	lockBlock 		*bitmap
	// the max size of cache is (blockSize * MaxInt32)
	blockSize		int32

	prefetchOnce    sync.Once

	// prefetch result
	prefetchRs		PreFetchResult
	prefetchCh      chan struct{}
}

func newSeed(base seedBaseOpt, rate rateOpt) (Seed, error) {
	if base.info.FullLength == 0 {
		return nil, fmt.Errorf("full size should be set")
	}

	if base.blockOrder < 10 || base.blockOrder > 31 {
		return nil, fmt.Errorf("block order should be [2,31]")
	}

	cache, err := newFileCacheBuffer(base.contentPath, base.info.FullLength, true)
	if err != nil {
		return nil, err
	}

	sd := &seed{
		Status:      INITIAL_STATUS,
		Url:         base.info.URL,
		Header:      base.info.Header,
		FullSize:	 base.info.FullLength,
		TaskId:      base.info.TaskID,
		BlockOrder:  base.blockOrder,
		cache:       cache,
		metaPath:    base.metaPath,
		metaBakPath: base.metaBakPath,
		ContentPath: base.contentPath,
		down: newLocalDownloader(base.info.URL, base.info.Header, rate.downloadRateLimiter),
		//uploadRate: sm.uploadRate,
		prefetchCh: make(chan struct{}),
	}

	sd.initParam()

	return sd, nil
}

// Prefetch will prefetch data to buffer, and its download rate will be limited.
func (sd *seed) Prefetch(perDownloadSize int64) (<- chan struct{}, error) {
	sd.Lock()
	defer sd.Unlock()

	if sd.Status != INITIAL_STATUS {
		return sd.prefetchCh, nil
	}

	sd.Status = FETCHING_STATUS

	go func() {
		sd.prefetchOnce.Do(func() {
			block := int32(perDownloadSize >> sd.BlockOrder)
			if block == 0 {
				block = 1
			}
			blockSize := int64(block) << sd.BlockOrder
			var start, end int64
			var err error
			var try int
			var maxTry int = 3

			for{
				if start >= sd.FullSize {
					break
				}

				end = start + blockSize - 1
				if end >= sd.FullSize {
					end = sd.FullSize - 1
				}

				err = sd.download(start, end, true)
				if err != nil {
					//todo: try again
					logrus.Errorf("failed to download: %v", err)
					if try > maxTry {
						err = fmt.Errorf("try %d times to download file %s, range (%d-%d) failed: %v",
							try, sd.Url, start, end, err)
						break
					}
					try ++
					continue
				}

				maxTry = 0
				start = end + 1
			}

			sd.Lock()
			defer sd.Unlock()

			if err != nil {
				sd.prefetchRs = PreFetchResult{
					Success: false,
					Err: err,
				}
				sd.Status = INITIAL_STATUS
			}else {
				sd.prefetchRs = PreFetchResult{
					Success: true,
					Err: nil,
				}
				sd.Status = FINISHED_STATUS
			}

			close(sd.prefetchCh)
		})
	}()

	return sd.prefetchCh, nil
}

func (sd *seed) GetPrefetchResult() (PreFetchResult, error) {
	sd.RLock()
	defer sd.RUnlock()

	if sd.Status == FETCHING_STATUS {
		return PreFetchResult{}, fmt.Errorf("prefetch not finished")
	}

	return sd.prefetchRs, nil
}

func (sd *seed) Delete() error {
	sd.Lock()
	defer sd.Unlock()

	if sd.Status == DEAD_STATUS {
		return nil
	}

	sd.clearResource()
	return nil
}

func (sd *seed) Download(off int64, size int64) (io.ReadCloser, error) {
	off, size, err := sd.checkReadStreamParam(off, size)
	if err != nil {
		return nil, err
	}

	err = sd.download(off, off + size - 1, false)
	if err != nil {
		return nil, err
	}

	return sd.cache.ReadStream(off, size)
}

func (sd *seed) GetFullSize() int64 {
	sd.RLock()
	defer sd.RUnlock()

	return sd.FullSize
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

func (sd *seed) Headers() map[string][]string {
	sd.RLock()
	defer sd.RUnlock()

	return sd.Header
}

func (sd *seed) initParam() {
	blockSize := 1 << sd.BlockOrder
	blocks := sd.FullSize/int64(blockSize)
	if (sd.FullSize % int64(blockSize)) > 0 {
		blocks ++
	}

	sizeOf64Bits := blocks/64
	if (blocks % 64) > 0 {
		sizeOf64Bits ++
	}
	// todo: restore from file
	sd.blockMeta = newBitMap(int32(sizeOf64Bits), false)
	sd.lockBlock = newBitMap(int32(sizeOf64Bits), false)
}

func (sd *seed) checkReadStreamParam(off int64, size int64) (int64, int64, error) {
	sd.RLock()
	defer sd.RUnlock()

	if sd.Status == DEAD_STATUS {
		return 0 ,0, fmt.Errorf("daed seed")
	}

	if off < 0 {
		off = 0
	}

	// if size <= 0, set range to [off, fullSize - 1]
	if size <= 0 {
		size = sd.FullSize - off
	}

	if off + size > sd.FullSize {
		return 0, 0, errortypes.NewHttpError(http.StatusRequestedRangeNotSatisfiable, "out of range")
	}

	return off, size, nil
}

// alignWithBlock will align bytes range to block size, and return the block range.
func (sd *seed) alignWithBlock(start int64, end int64) (int32, int32) {
	return int32(start >> sd.BlockOrder), int32(end >> sd.BlockOrder)
}

func (sd *seed) download(start, end int64, rateLimit bool) error {
	startBlock, endBlock := sd.alignWithBlock(start, end)
	fmt.Printf("start to download, start-end: [%d-%d], block[%d-%d]\n", start, end, startBlock, endBlock)

check:
	rs := sd.blockMeta.get(startBlock, endBlock, false)
	// if all bits is set, it means the range has been downloaded.
	if len(rs) == 0 {
		return nil
	}

	unsetRange := []*bitmapRs{}
	waitChs := []chan struct{}{}

	for _, r := range rs {
		downRs := sd.lockBlock.lockRange(r.startIndex, r.endIndex)
		unsetRange = append(unsetRange, downRs.unsetRange...)
		waitChs = append(waitChs, downRs.waitChs...)
	}
	// set defer func to release lock range
	for _, r := range unsetRange {
		defer func(b, e int32) {
			sd.lockBlock.unlockRange(b, e)
		}(r.startIndex, r.endIndex)
	}

	for _, r := range unsetRange {
		endBytes := (int64(r.endIndex + 1) << sd.BlockOrder) - 1
		if endBytes >= sd.FullSize {
			endBytes = sd.FullSize - 1
		}

		fmt.Printf("start to download file range [%d, %d]\n", int64(r.startIndex) << sd.BlockOrder, endBytes)
		err := sd.downloadToFile(int64(r.startIndex) << sd.BlockOrder, endBytes, rateLimit)
		if err != nil {
			return err
		}

		// set bits in bitmap to indicate the range has been downloaded.
		sd.blockMeta.set(r.startIndex, r.endIndex, true)
	}

	// wait for the chan
	for _, ch := range waitChs {
		// todo: set the timeout, if timeout, try to direct download again.
		select {
		case <- ch:
			break
		}
	}

	// check again, the wait range may be download failed
	goto check
}

func (sd *seed) downloadToFile(start, end int64, rateLimit bool) error {
	timeout := netutils.CalculateTimeout(end - start + 1, 0, config.DefaultMinRate, 10 * time.Second)
	_, err := sd.down.DownloadToWriterAt(context.Background(), httputils.RangeStruct{StartIndex: start, EndIndex: end},
		timeout, start, sd.cache, rateLimit)

	if err != nil {
		return err
	}

	return nil
}

func (sd *seed) clearResource() {
	os.Remove(sd.metaBakPath)
	os.Remove(sd.metaPath)
	sd.cache.Remove()
}
