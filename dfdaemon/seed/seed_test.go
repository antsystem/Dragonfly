package seed

import (
	"fmt"
	"github.com/dragonflyoss/Dragonfly/pkg/ratelimiter"
	"github.com/go-check/check"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"path/filepath"
	"sync"
	"time"
)

func (s *SeedTestSuite) checkDataWithFileServer(c *check.C, path string, off int64, size int64, obtained []byte) {
	expected, err := s.readFromFileServer(path, off, size)
	c.Assert(err, check.IsNil)
	if string(obtained) != string(expected) {
		c.Errorf("path %s, range [%d-%d]: get %s, expect %s", path, off, off + size - 1,
			string(obtained), string(expected))
	}

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
	rc, err = sd.Download(fileLength-100*1024, 100*1024)
	c.Assert(err, check.IsNil)
	obtainedData, err = ioutil.ReadAll(rc)
	rc.Close()
	c.Assert(err, check.IsNil)
	s.checkDataWithFileServer(c, path, fileLength-100*1024, 100*1024, obtainedData)

	// download 0-{100KB-1}
	rc, err = sd.Download(0, 100*1024)
	c.Assert(err, check.IsNil)
	obtainedData, err = ioutil.ReadAll(rc)
	rc.Close()
	c.Assert(err, check.IsNil)
	s.checkDataWithFileServer(c, path, 0, 100*1024, obtainedData)

	start := int64(0)
	end := int64(0)
	rangeSize := int64(100 * 1024)

	for {
		end = start + rangeSize - 1
		if end >= fileLength {
			end = fileLength - 1
		}

		if start > end {
			break
		}

		rc, err = sd.Download(start, end-start+1)
		c.Assert(err, check.IsNil)
		obtainedData, err = ioutil.ReadAll(rc)
		rc.Close()
		c.Assert(err, check.IsNil)
		s.checkDataWithFileServer(c, path, start, end-start+1, obtainedData)
		start = end + 1
	}

	start = 0
	end = 0
	rangeSize = 99 * 1023

	for {
		end = start + rangeSize - 1
		if end >= fileLength {
			end = fileLength - 1
		}

		if start > end {
			break
		}

		rc, err = sd.Download(start, end-start+1)
		c.Assert(err, check.IsNil)
		obtainedData, err = ioutil.ReadAll(rc)
		rc.Close()
		c.Assert(err, check.IsNil)
		s.checkDataWithFileServer(c, path, start, end-start+1, obtainedData)
		start = end + 1
	}
}

func (s *SeedTestSuite) checkSeedFile(c *check.C, path string, fileLength int64, seedName string, order uint32, perDownloadSize int64, wg *sync.WaitGroup) {
	defer func() {
		if wg != nil {
			wg.Done()
		}
	}()

	contentPath := filepath.Join(s.cacheDir, fmt.Sprintf("%s-content", seedName))
	metaPath := filepath.Join(s.cacheDir, fmt.Sprintf("%s-meta", seedName))
	metaBakPath := filepath.Join(s.cacheDir, fmt.Sprintf("%s-meta.bak", seedName))
	blockOrder := uint32(order)

	sOpt := seedBaseOpt{
		contentPath: contentPath,
		metaPath: metaPath,
		metaBakPath: metaBakPath,
		blockOrder: blockOrder,
		info:  &PreFetchInfo{
			URL: fmt.Sprintf("http://%s/%s", s.host, path),
			TaskID: uuid.New(),
			FullLength: fileLength,
		},
	}

	sd, err := newSeed(sOpt, rateOpt{downloadRateLimiter: ratelimiter.NewRateLimiter(0, 0)})
	c.Assert(err, check.IsNil)

	finishCh, err := sd.Prefetch(perDownloadSize)
	c.Assert(err, check.IsNil)

	 <-finishCh
	 rs, err := sd.GetPrefetchResult()
	 c.Assert(err, check.IsNil)
	c.Assert(rs.Success, check.Equals, true)
	c.Assert(rs.Err, check.IsNil)

	c.Assert(sd.GetFullSize(), check.Equals, fileLength)
	s.checkFileWithSeed(c, path, fileLength, sd)
}

//func (s *SeedTestSuite) TestNormalSeed(c *check.C) {
//	urlA := fmt.Sprintf("http://%s/fileA", s.host)
//	contentPath := filepath.Join(s.cacheDir, "TestSeedNormalContent1")
//	metaPath := filepath.Join(s.cacheDir, "TestSeedNormalMeta1")
//	metaBakPath := filepath.Join(s.cacheDir, "TestSeedNormalMetaBak1")
//	// 8 KB
//	blockOrder := uint32(13)
//
//	sOpt := seedBaseOpt{
//		contentPath: contentPath,
//		metaPath: metaPath,
//		metaBakPath: metaBakPath,
//		blockOrder: blockOrder,
//		info:  &PreFetchInfo{
//			URL: urlA,
//			TaskID: uuid.New(),
//			FullLength: 500*1024,
//		},
//	}
//
//	sd, err := newSeed(sOpt, rateOpt{downloadRateLimiter: ratelimiter.NewRateLimiter(0, 0)})
//	c.Assert(err, check.IsNil)
//
//	notifyCh, err := sd.Prefetch(16 * 1024)
//	c.Assert(err, check.IsNil)
//
//	// wait for prefetch ok
//	<- notifyCh
//	rs, err := sd.GetPrefetchResult()
//	c.Assert(err, check.IsNil)
//	c.Assert(rs.Success, check.Equals, true)
//	c.Assert(rs.Err , check.IsNil)
//
//	s.checkFileWithSeed(c, "fileA", 500*1024, sd)
//
//	s.checkSeedFile(c, "fileB", 1024*1024, "TestNormalSeed-fileB", 14, 17 * 1024, nil)
//	s.checkSeedFile(c, "fileC", 1500*1024, "TestNormalSeed-fileC", 12, 10 * 1024, nil)
//	s.checkSeedFile(c, "fileD", 2048*1024, "TestNormalSeed-fileD", 13, 24 * 1024, nil)
//	s.checkSeedFile(c, "fileE", 9500*1024, "TestNormalSeed-fileE", 15, 100 * 1024, nil)
//	s.checkSeedFile(c, "fileF", 10*1024*1024, "TestNormalSeed-fileE", 15, 96 * 1024, nil)
//}
//
//func (s *SeedTestSuite) TestSeedSyncRead(c *check.C) {
//	var(
//		start, end, rangeSize int64
//	)
//
//	urlF := fmt.Sprintf("http://%s/fileF", s.host)
//	contentPath := filepath.Join(s.cacheDir, "TestSeedSyncReadContent1")
//	metaPath := filepath.Join(s.cacheDir, "TestSeedSyncReadMeta1")
//	metaBakPath := filepath.Join(s.cacheDir, "TestSeedSyncReadMetaBak1")
//	// 64 KB
//	blockOrder := uint32(16)
//
//	sOpt := seedBaseOpt{
//		contentPath: contentPath,
//		metaPath: metaPath,
//		metaBakPath: metaBakPath,
//		blockOrder: blockOrder,
//		info:  &PreFetchInfo{
//			URL: urlF,
//			TaskID: uuid.New(),
//			FullLength: 10*1024*1024,
//		},
//	}
//
//	now := time.Now()
//
//	sd, err := newSeed(sOpt, rateOpt{downloadRateLimiter: ratelimiter.NewRateLimiter(0, 0)})
//	c.Assert(err, check.IsNil)
//
//	notifyCh, err := sd.Prefetch(64 * 1024)
//	c.Assert(err, check.IsNil)
//
//	// try to download
//	start = 0
//	end = 0
//	rangeSize = 99 * 1023
//	fileLength := int64(10*1024*1024)
//
//	for {
//		end = start + rangeSize - 1
//		if end >= fileLength {
//			end = fileLength - 1
//		}
//
//		if start > end {
//			break
//		}
//
//		startTime := time.Now()
//		rc, err := sd.Download(start, end-start+1)
//		logrus.Infof("in TestSeedSyncRead, Download 100KB costs time: %f second", time.Now().Sub(startTime).Seconds())
//		c.Assert(err, check.IsNil)
//		obtainedData, err := ioutil.ReadAll(rc)
//		rc.Close()
//		c.Assert(err, check.IsNil)
//
//		startTime = time.Now()
//		_, err = s.readFromFileServer("fileF", start, end-start+1)
//		c.Assert(err, check.IsNil)
//		logrus.Infof("in TestSeedSyncRead, Download from source 100KB costs time: %f second", time.Now().Sub(startTime).Seconds())
//
//		s.checkDataWithFileServer(c, "fileF", start, end-start+1, obtainedData)
//		start = end + 1
//	}
//
//	<- notifyCh
//	logrus.Infof("in TestSeedSyncRead, costs time: %f second", time.Now().Sub(now).Seconds())
//
//	rs, err := sd.GetPrefetchResult()
//	c.Assert(err, check.IsNil)
//	c.Assert(rs.Success, check.Equals, true)
//	c.Assert(rs.Err , check.IsNil)
//
//	s.checkFileWithSeed(c, "fileF", fileLength, sd)
//}

func (s *SeedTestSuite) TestSeedSyncReadPerformance(c *check.C) {
	var(
		rangeSize int64
	)

	fileName := "fileA"
	fileLength := int64(500*1024)
	urlF := fmt.Sprintf("http://%s/%s", s.host, fileName)
	contentPath := filepath.Join(s.cacheDir, "TestSeedSyncReadPerformanceContent1")
	metaPath := filepath.Join(s.cacheDir, "TestSeedSyncReadPerformanceMeta1")
	metaBakPath := filepath.Join(s.cacheDir, "TestSeedSyncReadPerformanceMetaBak1")
	// 16 KB
	blockOrder := uint32(16)
	sOpt := seedBaseOpt{
		contentPath: contentPath,
		metaPath: metaPath,
		metaBakPath: metaBakPath,
		blockOrder: blockOrder,
		info:  &PreFetchInfo{
			URL: urlF,
			TaskID: uuid.New(),
			FullLength: fileLength,
		},
	}

	now := time.Now()

	sd, err := newSeed(sOpt, rateOpt{downloadRateLimiter: ratelimiter.NewRateLimiter(0, 0)})
	c.Assert(err, check.IsNil)

	//notifyCh, err := sd.Prefetch(64 * 1024)
	//c.Assert(err, check.IsNil)

	wg := &sync.WaitGroup{}

	// try to download in 20 goroutine
	for i := 0; i < 5; i ++ {
		rangeSize = 9 * 1023
		wg.Add(1)
		go func() {
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

				startTime := time.Now()
				rc, err := sd.Download(start, end-start+1)
				logrus.Infof("in TestSeedSyncReadPerformance, Download 100KB costs time: %f second", time.Now().Sub(startTime).Seconds())
				c.Assert(err, check.IsNil)
				obtainedData, err := ioutil.ReadAll(rc)
				rc.Close()
				c.Assert(err, check.IsNil)

				startTime = time.Now()
				_, err = s.readFromFileServer(fileName, start, end-start+1)
				c.Assert(err, check.IsNil)
				logrus.Infof("in TestSeedSyncReadPerformance, Download from source 100KB costs time: %f second", time.Now().Sub(startTime).Seconds())

				s.checkDataWithFileServer(c, fileName, start, end-start+1, obtainedData)
				start = end + 1
			}
		}()
	}

	//<- notifyCh
	logrus.Infof("in TestSeedSyncRead, costs time: %f second", time.Now().Sub(now).Seconds())

	wg.Wait()

	//rs, err := sd.GetPrefetchResult()
	//c.Assert(err, check.IsNil)
	//c.Assert(rs.Success, check.Equals, true)
	//c.Assert(rs.Err , check.IsNil)

	//s.checkFileWithSeed(c, "fileF", fileLength, sd)
}
