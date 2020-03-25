package seed

import(
	"fmt"
	"github.com/dragonflyoss/Dragonfly/pkg/ratelimiter"
	"github.com/go-check/check"
	"github.com/pborman/uuid"
	"io/ioutil"
	"sync"
	"time"
)

func (s *SeedTestSuite) checkDataWithFileServer(c *check.C, path string, off int64, size int64, obtained []byte) {
	expected, err := s.readFromFileServer(path, off, size)
	c.Assert(err, check.IsNil)

	c.Assert(string(obtained), check.Equals, string(expected))
}

func (s *SeedTestSuite) checkFileWithSeed(c *check.C, path string, fileLength int64, sd Seed) {
	// download all
	rc, err := sd.Download(0, -1)
	c.Assert(err, check.IsNil)
	obtainedData, err := ioutil.ReadAll(rc)
	rc.Close()
	c.Assert(err, check.IsNil)
	s.checkDataWithFileServer(c, path, 0, -1, obtainedData)

	// download {fileLength-100KB}- {fileLength}-1
	rc, err = sd.Download(fileLength - 100 * 1024, 100 * 1024)
	c.Assert(err, check.IsNil)
	obtainedData, err = ioutil.ReadAll(rc)
	rc.Close()
	c.Assert(err, check.IsNil)
	s.checkDataWithFileServer(c, path, fileLength - 100 * 1024, 100 * 1024, obtainedData)

	// download 0-{100KB-1}
	rc, err = sd.Download(0, 100 * 1024)
	c.Assert(err, check.IsNil)
	obtainedData, err = ioutil.ReadAll(rc)
	rc.Close()
	c.Assert(err, check.IsNil)
	s.checkDataWithFileServer(c, path, 0, 100 * 1024, obtainedData)

	start := int64(0)
	end := int64(0)
	rangeSize := int64(100 * 1024)

	for{
		end = start + rangeSize - 1
		if end >= fileLength {
			end = fileLength - 1
		}

		if start > end {
			break
		}

		rc, err = sd.Download(start, end - start + 1)
		c.Assert(err, check.IsNil)
		obtainedData, err = ioutil.ReadAll(rc)
		rc.Close()
		c.Assert(err, check.IsNil)
		s.checkDataWithFileServer(c, path, start, end - start + 1, obtainedData)
		start = end + 1
	}

	start = 0
	end = 0
	rangeSize = 99 * 1023

	for{
		end = start + rangeSize - 1
		if end >= fileLength {
			end = fileLength - 1
		}

		if start > end {
			break
		}

		rc, err = sd.Download(start, end - start + 1)
		c.Assert(err, check.IsNil)
		obtainedData, err = ioutil.ReadAll(rc)
		rc.Close()
		c.Assert(err, check.IsNil)
		s.checkDataWithFileServer(c, path, start, end - start + 1, obtainedData)
		start = end + 1
	}
}

func (s *SeedTestSuite) checkSeedFile(c *check.C, path string, fileLength int64, sd *seed, wg *sync.WaitGroup) {
	defer func() {
		if wg != nil {
			wg.Done()
		}
	}()

	finishCh, err := sd.Prefetch(ratelimiter.NewRateLimiter(0, 0), 30 * time.Second)
	c.Assert(err, check.IsNil)

	prefetchResult := <- finishCh
	c.Assert(prefetchResult.Success, check.Equals, true)
	c.Assert(prefetchResult.Canceled, check.Equals, false)
	c.Assert(prefetchResult.Err, check.IsNil)

	c.Assert(sd.getHttpFileLength(), check.Equals, fileLength)
	s.checkFileWithSeed(c, path, fileLength, sd)
}

func (s *SeedTestSuite) TestOneSeed(c *check.C) {
	sm, err := newSeedManager(s.cacheDir, 2, 10, 1024 * 1024)
	c.Assert(err, check.IsNil)

	preInfo := &PreFetchInfo{
		// fileA: 500KB
		URL: fmt.Sprintf("http://%s/%s", s.host, "fileA"),
	}

	taskID := uuid.New()
	sd, err := sm.Register(taskID, preInfo)
	c.Assert(err, check.IsNil)

	finishCh, err := sd.Prefetch(ratelimiter.NewRateLimiter(0, 0), 30 * time.Second)
	c.Assert(err, check.IsNil)

	prefetchResult := <- finishCh
	c.Assert(prefetchResult.Success, check.Equals, true)
	c.Assert(prefetchResult.Canceled, check.Equals, false)
	c.Assert(prefetchResult.Err, check.IsNil)

	// download all
	rc, err := sd.Download(0, -1)
	c.Assert(err, check.IsNil)
	obtainedData, err := ioutil.ReadAll(rc)
	rc.Close()
	c.Assert(err, check.IsNil)
	s.checkDataWithFileServer(c, "fileA", 0, -1, obtainedData)

	// download 0-100*1024
	rc, err = sd.Download(0, 100 * 1024)
	c.Assert(err, check.IsNil)
	obtainedData, err = ioutil.ReadAll(rc)
	rc.Close()
	c.Assert(err, check.IsNil)
	s.checkDataWithFileServer(c, "fileA", 0, 100 * 1024, obtainedData)

	// download 100*1024-(500*1024-1)
	rc, err = sd.Download(100*1024, 400 * 1024)
	c.Assert(err, check.IsNil)
	obtainedData, err = ioutil.ReadAll(rc)
	rc.Close()
	c.Assert(err, check.IsNil)
	s.checkDataWithFileServer(c, "fileA", 100*1024, 400 * 1024, obtainedData)

	s.checkFileWithSeed(c, "fileA", 500 * 1024 , sd)

	// try to gc
	time.Sleep(time.Second * 31)
	lsm := sm.(*seedManager)
	// gc expired  seed
	lsm.gcExpiredSeed()

	_, err = sd.Download(0, -1)
	c.Assert(err, check.NotNil)

	expiredCh, err := sd.NotifyExpired()
	c.Assert(err, check.IsNil)
	receiveExpired := false
	select {
		case <- expiredCh:
			receiveExpired = true
		default:
	}

	c.Assert(receiveExpired, check.Equals, true)

	_, err = sm.Get(taskID)
	c.Assert(err, check.NotNil)
}

func (s *SeedTestSuite) TestManySeed(c *check.C) {
	sm, err := newSeedManager(s.cacheDir, 2, 4, 1024 * 1024)
	c.Assert(err, check.IsNil)

	filePaths := []string{"fileB", "fileC", "fileD", "fileE", "fileF"}
	fileLens := []int64{1024 * 1024, 1500 * 1024, 2048 * 1024, 9500 * 1024, 10 * 1024 * 1024}
	taskIDArr := make([]string, 5)
	seedArr := make([]Seed, 5)

	wg := &sync.WaitGroup{}
	for i:= 0; i < 4; i ++ {
		wg.Add(1)
		taskIDArr[i] = uuid.New()
		sd, err := sm.Register(taskIDArr[i], &PreFetchInfo{URL: fmt.Sprintf("http://%s/%s", s.host, filePaths[i]),})
		c.Assert(err, check.IsNil)
		seedArr[i] = sd

		go func(lsd Seed, path string, fileLength int64) {
			s.checkSeedFile(c, path, fileLength, lsd.(*seed), wg)
		}(sd, filePaths[i], fileLens[i])
	}

	wg.Wait()
	// refresh expired time
	for i:= 1; i < 4; i ++ {
		rc, err := seedArr[i].Download(0, 10)
		c.Assert(err, check.IsNil)
		rc.Close()
	}

	// new one seed, it may wide out the oldest one
	taskIDArr[4] = uuid.New()
	seedArr[4], err = sm.Register(taskIDArr[4], &PreFetchInfo{URL: fmt.Sprintf("http://%s/%s", s.host, filePaths[4]),})
	c.Assert(err, check.IsNil)
	s.checkSeedFile(c, filePaths[4], fileLens[4], seedArr[4].(*seed), nil)

	// check the oldest one taskIDArr[0], it should be wide out
	_, err = sm.Get(taskIDArr[0])
	c.Assert(err, check.NotNil)

	expiredCh, err := seedArr[0].NotifyExpired()
	c.Assert(err, check.IsNil)
	receiveExpired := false
	select {
	case <- expiredCh:
		receiveExpired = true
	default:
	}

	c.Assert(receiveExpired, check.Equals, true)

	// refresh taskIDArr[1]
	seedArr[1].RefreshExpiredTime(0)
	time.Sleep(35 * time.Second)
	lsm := sm.(*seedManager)
	// gc expired seed
	lsm.gcExpiredSeed()

	for i:= 2; i < 5; i ++ {
		// check the oldest one taskIDArr[i], it should be wide out
		_, err = sm.Get(taskIDArr[i])
		c.Assert(err, check.NotNil)

		expiredCh, err := seedArr[i].NotifyExpired()
		c.Assert(err, check.IsNil)
		receiveExpired := false
		select {
		case <- expiredCh:
			receiveExpired = true
		default:
		}

		c.Assert(receiveExpired, check.Equals, true)
	}
}
