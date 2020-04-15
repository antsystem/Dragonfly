package seed

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly/dfget/core/helper"
	"github.com/dragonflyoss/Dragonfly/pkg/ratelimiter"
	"github.com/go-check/check"
	"github.com/pborman/uuid"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func Test(t *testing.T) {
	check.TestingT(t)
}

type SeedTestSuite struct {
	port     int
	host     string
	server   *helper.MockFileServer
	tmpDir   string
	cacheDir string
}

func init() {
	rand.Seed(time.Now().Unix())
	check.Suite(&SeedTestSuite{})
}

func (suite *SeedTestSuite) SetUpSuite(c *check.C) {
	suite.tmpDir = "./testdata"
	err := os.MkdirAll(suite.tmpDir, 0774)
	c.Assert(err, check.IsNil)

	suite.cacheDir = "./testcache"
	err = os.MkdirAll(suite.cacheDir, 0774)
	c.Assert(err, check.IsNil)

	suite.port = rand.Intn(1000) + 63000
	suite.host = fmt.Sprintf("127.0.0.1:%d", suite.port)

	suite.server = helper.NewMockFileServer()
	err = suite.server.StartServer(context.Background(), suite.port)
	c.Assert(err, check.IsNil)

	// 500KB
	err = suite.server.RegisterFile("fileA", 500*1024, "abcde0123456789")
	c.Assert(err, check.IsNil)
	// 1MB
	err = suite.server.RegisterFile("fileB", 1024*1024, "abcdefg")
	c.Assert(err, check.IsNil)
	// 1.5 MB
	err = suite.server.RegisterFile("fileC", 1500*1024, "abcdefg")
	c.Assert(err, check.IsNil)
	// 2 MB
	err = suite.server.RegisterFile("fileD", 2048*1024, "abcdefg")
	c.Assert(err, check.IsNil)
	// 9.5 MB
	err = suite.server.RegisterFile("fileE", 9500*1024, "abcdefg")
	c.Assert(err, check.IsNil)
	// 10 MB
	err = suite.server.RegisterFile("fileF", 10*1024*1024, "abcdefg")
	c.Assert(err, check.IsNil)
	// 1 G
	err = suite.server.RegisterFile("fileG", 1024*1024*1024, "1abcdefg")
	c.Assert(err, check.IsNil)

	// 100 M
	err = suite.server.RegisterFile("fileH", 100*1024*1024, "1abcdefg")
	c.Assert(err, check.IsNil)
}

func (suite *SeedTestSuite) TearDownSuite(c *check.C) {
	if suite.tmpDir != "" {
		os.RemoveAll(suite.tmpDir)
	}
	if suite.cacheDir != "" {
		os.RemoveAll(suite.cacheDir)
	}
}

func (suite *SeedTestSuite) checkDataWithFileServer(c *check.C, path string, off int64, size int64, obtained []byte) {
	expected, err := suite.readFromFileServer(path, off, size)
	c.Assert(err, check.IsNil)
	if string(obtained) != string(expected) {
		c.Errorf("path %s, range [%d-%d]: get %s, expect %s", path, off, off + size - 1,
			string(obtained), string(expected))
	}

	c.Assert(string(obtained), check.Equals, string(expected))
}

func (suite *SeedTestSuite) checkFileWithSeed(c *check.C, path string, fileLength int64, sd Seed) {
	// download all
	rc, err := sd.Download(0, -1)
	c.Assert(err, check.IsNil)
	obtainedData, err := ioutil.ReadAll(rc)
	rc.Close()
	c.Assert(err, check.IsNil)
	suite.checkDataWithFileServer(c, path, 0, -1, obtainedData)

	// download {fileLength-100KB}- {fileLength}-1
	rc, err = sd.Download(fileLength-100*1024, 100*1024)
	c.Assert(err, check.IsNil)
	obtainedData, err = ioutil.ReadAll(rc)
	rc.Close()
	c.Assert(err, check.IsNil)
	suite.checkDataWithFileServer(c, path, fileLength-100*1024, 100*1024, obtainedData)

	// download 0-{100KB-1}
	rc, err = sd.Download(0, 100*1024)
	c.Assert(err, check.IsNil)
	obtainedData, err = ioutil.ReadAll(rc)
	rc.Close()
	c.Assert(err, check.IsNil)
	suite.checkDataWithFileServer(c, path, 0, 100*1024, obtainedData)

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
		suite.checkDataWithFileServer(c, path, start, end-start+1, obtainedData)
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
		suite.checkDataWithFileServer(c, path, start, end-start+1, obtainedData)
		start = end + 1
	}
}

func (suite *SeedTestSuite) checkSeedFile(c *check.C, path string, fileLength int64, seedName string, order uint32, perDownloadSize int64, wg *sync.WaitGroup) {
	defer func() {
		if wg != nil {
			wg.Done()
		}
	}()

	metaDir := filepath.Join(suite.cacheDir, seedName)
	blockOrder := uint32(order)

	sOpt := SeedBaseOpt{
		MetaDir: metaDir,
		Info:  PreFetchInfo{
			URL: fmt.Sprintf("http://%s/%s", suite.host, path),
			TaskID: uuid.New(),
			FullLength: fileLength,
			BlockOrder:  blockOrder,
		},
	}

	sd, err := NewSeed(sOpt, RateOpt{DownloadRateLimiter: ratelimiter.NewRateLimiter(0, 0)}, false)
	c.Assert(err, check.IsNil)

	finishCh, err := sd.Prefetch(perDownloadSize)
	c.Assert(err, check.IsNil)

	<-finishCh
	rs, err := sd.GetPrefetchResult()
	c.Assert(err, check.IsNil)
	c.Assert(rs.Success, check.Equals, true)
	c.Assert(rs.Err, check.IsNil)

	c.Assert(sd.GetFullSize(), check.Equals, fileLength)
	suite.checkFileWithSeed(c, path, fileLength, sd)
}

func (suite *SeedTestSuite) checkSeedFileBySeedManager(c *check.C, path string, fileLength int64, seedName string, perDownloadSize int64, sd Seed, sm SeedManager, wg *sync.WaitGroup) {
	defer func() {
		if wg != nil {
			wg.Done()
		}
	}()

	finishCh, err := sm.Prefetch(seedName, perDownloadSize)
	c.Assert(err, check.IsNil)

	<-finishCh
	rs, err := sd.GetPrefetchResult()
	c.Assert(err, check.IsNil)
	c.Assert(rs.Success, check.Equals, true)
	c.Assert(rs.Err, check.IsNil)

	c.Assert(sd.GetFullSize(), check.Equals, fileLength)
	suite.checkFileWithSeed(c, path, fileLength, sd)
}

func (suite *SeedTestSuite) TestUintArrayEncodeAndDeCode(c *check.C) {
	arr1 := []uint64{0,1,2,3,4,5,10000}
	data1 := EncodeUintArray(arr1)

	deArr1 := DecodeToUintArray(data1)
	c.Assert(len(deArr1), check.Equals, len(arr1))

	for i := 0; i < len(arr1); i ++ {
		c.Assert(arr1[i], check.Equals, deArr1[i])
	}

	arr2 := make([]uint64, 1000000)
	for  i := 0; i < 1000000; i ++ {
		arr2[i] = rand.Uint64()
	}

	now := time.Now()
	data2 := EncodeUintArray(arr2)
	fmt.Printf("encode 1000000 uint64 array, costs %f seconds\n", time.Now().Sub(now).Seconds())

	now = time.Now()
	deArr2 := DecodeToUintArray(data2)
	fmt.Printf("decode 1000000 uint64 array, costs %f seconds\n", time.Now().Sub(now).Seconds())
	c.Assert(len(deArr2), check.Equals, len(arr2))

	for i := 0; i < len(arr2); i ++ {
		c.Assert(arr2[i], check.Equals, deArr2[i])
	}
}
