package client

import (
	"fmt"
	"github.com/dragonflyoss/Dragonfly/dfdaemon/seed"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"github.com/dragonflyoss/Dragonfly/pkg/ratelimiter"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

func init()  {
	logrus.SetLevel(logrus.DebugLevel)
}

func Run(host string, cacheDir string, directSrc bool, cacheRun bool, fileCache bool)  {
	var(
		rangeSize  int64 = 99 * 1023
	)

	fileName := "fileG"
	fileLength := int64(100*1024*1024)
	urlF := fmt.Sprintf("http://%s/%s",host, fileName)
	metaDir := filepath.Join(cacheDir, "seed")
	// 128 KB
	blockOrder := uint32(17)
	sOpt := seed.SeedBaseOpt{
		BaseDir:    metaDir,
		Info:  seed.PreFetchInfo{
			URL: urlF,
			TaskID: uuid.New(),
			FullLength: fileLength,
			BlockOrder: blockOrder,
		},
	}

	now := time.Now()

	sd, err := seed.NewSeed(sOpt, seed.RateOpt{DownloadRateLimiter: ratelimiter.NewRateLimiter(0, 0)}, true)
	if err != nil {
		panic(err)
	}

	//notifyCh, err := sd.Prefetch(128 * 1024)
	//c.Assert(err, check.IsNil)

	wg := &sync.WaitGroup{}

	if !fileCache {
		// try to download in 20 goroutine
		for i := 0; i < 1; i++ {

			wg.Add(1)
			go runRequest(host, fileName, rangeSize, fileLength, directSrc, sd, wg)
		}
	}else{
		notifyCh, err := sd.Prefetch(128 * 1024)
		if  err != nil {
			panic(err)
		}
		<- notifyCh
	}

	//<- notifyCh
	logrus.Infof("in TestSeedSyncRead, costs time: %f second", time.Now().Sub(now).Seconds())

	wg.Wait()

	if !cacheRun {
		return
	}

	wg.Add(1)
	runRequest(host, fileName, rangeSize, fileLength, directSrc, sd, wg)
	wg.Wait()

	//_, err := sd.GetPrefetchResult()
}

func ReadFromFileServer(host, path string, off int64, size int64) ([]byte, error) {
	url := fmt.Sprintf("http://%s/%s", host, path)
	header := map[string]string{}

	if size > 0 {
		header["Range"] = fmt.Sprintf("bytes=%d-%d", off, off+size-1)
	}

	//code, data, err := httputils.GetWithHeaders(url, header, 5*time.Second)
	//if err != nil {
	//	return nil, err
	//}
	//
	//if code >= 400 {
	//	return nil, fmt.Errorf("resp code %d", code)
	//}

	resp, err := httputils.HTTPWithHeaders("GET", url, header, 5*time.Second, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("resp code %d", resp.StatusCode)
	}

	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)

	return data, err
}

func CheckDataWithFileServer(host, path string, off int64, size int64, obtained []byte) {
	expected, err := ReadFromFileServer(host, path, off, size)
	if err != nil {
		panic(fmt.Errorf("path %s, range [%d-%d], read failed: %v",path, off, off + size -1))
	}
	if string(obtained) != string(expected) {
		panic(fmt.Errorf("path %s, range [%d-%d]: get %s, expect %s", path, off, off + size - 1,
			string(obtained), string(expected)))
	}
}

func runRequest(host, fileName string, rangeSize int64, fileLength int64, directSrc bool, sd seed.Seed, wg *sync.WaitGroup)  {
	cc  := &CostsCounter{arr: []time.Duration{}}

	defer wg.Done()
	start := int64(0)
	end := int64(0)

	for {
		end = start + rangeSize - 1
		if end >= fileLength {
			end = fileLength - 1
		}

		if start > end {
			break
		}

		if !directSrc {
			startTime := time.Now()
			rc, err := sd.Download(start, end-start+1)
			if err != nil {
				panic(fmt.Errorf("path: %s, range:[%d,%d], failed to download: %v", fileName, start, end, err))
			}
			costs := time.Now().Sub(startTime)
			logrus.Infof("in TestSeedSyncReadPerformance, Download 100KB costs time: %f second", costs.Seconds())
			cc.Add(costs)

			obtainedData, err := ioutil.ReadAll(rc)
			rc.Close()
			if err != nil {
				panic(fmt.Errorf("path: %s, range:[%d,%d], failed to read from rc: %v", fileName, start, end, err))
			}

			CheckDataWithFileServer(host, fileName, start, end-start+1, obtainedData)
		}else{
			startTime := time.Now()
			obtainedData, _ := ReadFromFileServer(host, fileName, start, end-start+1)
			costs := time.Now().Sub(startTime)
			cc.Add(costs)
			logrus.Infof("in TestSeedSyncReadPerformance, Download from source 100KB costs time: %f second", costs.Seconds())
			CheckDataWithFileServer(host, fileName, start, end-start+1, obtainedData)
		}
		start = end + 1
	}

	logrus.Infof("means costs: %f second, count: %d", cc.Mean().Seconds(), cc.Len())
	logrus.Infof("c99: %f second, c95: %f second, c90: %f second, c80: %f second, c50: %f second, c20: %f second, c10: %f second",
		cc.CRate(99).Seconds(), cc.CRate(95).Seconds(), cc.CRate(90).Seconds(), cc.CRate(80).Seconds(), cc.CRate(50).Seconds(),
		cc.CRate(20).Seconds(), cc.CRate(10).Seconds())
}

type CostsCounter struct {
	sync.Mutex
	arr 	[]time.Duration
	sum		time.Duration
}

func (cc *CostsCounter) Add(costs time.Duration) {
	cc.Lock()
	defer cc.Unlock()

	cc.arr = append(cc.arr, costs)
	cc.sum += costs
}

func (cc *CostsCounter) Mean() time.Duration {
	cc.Lock()
	defer cc.Unlock()

	if len(cc.arr) == 0 {
		return 0
	}

	return time.Duration(int64(cc.sum)/int64(len(cc.arr)))
}

func (cc *CostsCounter) Len() int {
	cc.Lock()
	defer cc.Unlock()

	return len(cc.arr)
}

func (cc *CostsCounter) CRate(rate int) time.Duration {
	cc.Lock()
	defer cc.Unlock()

	sort.Slice(cc.arr, func(i, j int) bool {
		if cc.arr[i] < cc.arr[j] {
			return true
		}

		return false
	})

	index99 := len(cc.arr) * rate / 100
	return cc.arr[index99]
}
