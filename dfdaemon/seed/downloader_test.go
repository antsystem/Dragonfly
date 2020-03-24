package seed

import (
	"bytes"
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly/dfget/core/helper"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"github.com/dragonflyoss/Dragonfly/pkg/ratelimiter"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/go-check/check"
)

func Test(t *testing.T) {
	check.TestingT(t)
}

type SeedTestSuite struct {
	port int
	host string
	server *helper.MockFileServer
	tmpDir	string
	cacheDir string
}

func init() {
	rand.Seed(time.Now().Unix())
	check.Suite(&SeedTestSuite{})
}

func (s *SeedTestSuite) SetUpSuite(c *check.C) {
	s.tmpDir = "./testdata"
	err := os.MkdirAll(s.tmpDir, 0774)
	c.Assert(err, check.IsNil)

	s.cacheDir = "./testcache"
	err = os.MkdirAll(s.tmpDir, 0774)
	c.Assert(err, check.IsNil)

	s.port = rand.Intn(1000) + 63000
	s.host = fmt.Sprintf("127.0.0.1:%d", s.port)

	s.server = helper.NewMockFileServer()
	err = s.server.StartServer(context.Background(), s.port)
	c.Assert(err, check.IsNil)

	// 500KB
	err = s.server.RegisterFile("fileA", 500 * 1024, "abcdefg")
	c.Assert(err, check.IsNil)
	// 1MB
	err = s.server.RegisterFile("fileB", 1024 * 1024, "abcdefg")
	c.Assert(err, check.IsNil)
	// 1.5 MB
	err = s.server.RegisterFile("fileC", 1500 * 1024, "abcdefg")
	c.Assert(err, check.IsNil)
	// 2 MB
	err = s.server.RegisterFile("fileD", 2048 * 1024, "abcdefg")
	c.Assert(err, check.IsNil)
	// 9.5 MB
	err = s.server.RegisterFile("fileE", 9500 * 1024, "abcdefg")
	c.Assert(err, check.IsNil)
	// 10 MB
	err = s.server.RegisterFile("fileF", 10 * 1024 * 1024, "abcdefg")
	c.Assert(err, check.IsNil)

}

func (s *SeedTestSuite) TearDownSuite(c *check.C) {
	if s.tmpDir != "" {
		os.RemoveAll(s.tmpDir)
	}
	if s.cacheDir != "" {
		os.RemoveAll(s.cacheDir)
	}
}

func (s *SeedTestSuite) readFromFileServer(path string, off int64, size int64) ([]byte, error) {
	url := fmt.Sprintf("http://%s/%s", s.host, path)
	header := map[string]string{}

	if size > 0 {
		header["Range"] = fmt.Sprintf("bytes=%d-%d", off, off + size - 1)
	}

	code, data, err := httputils.GetWithHeaders(url, header, 5 * time.Second)
	if err != nil {
		return nil, err
	}

	if code >= 400 {
		return nil, fmt.Errorf("resp code %d", code)
	}

	return data, nil
}

func (s *SeedTestSuite) checkLocalDownloadDataFromFileServer(c *check.C, path string, off int64, size int64) {
	buf := bytes.NewBuffer(nil)
	ld := newLocalDownloader(fmt.Sprintf("http://%s/%s", s.host, path), nil, ratelimiter.NewRateLimiter(0, 0))

	length, err := ld.Download(context.Background(), httputils.RangeStruct{StartIndex: off, EndIndex: off + size -1}, 0, buf)
	c.Check(err, check.IsNil)
	c.Check(size, check.Equals, length)

	expectData, err := s.readFromFileServer(path, off, size)
	c.Check(err, check.IsNil)
	c.Check(string(buf.Bytes()), check.Equals, string(expectData))
}


func (s *SeedTestSuite) TestLocalDownload(c *check.C) {
	// test read fileA
	s.checkLocalDownloadDataFromFileServer(c, "fileA", 0, 500 * 1024)
	s.checkLocalDownloadDataFromFileServer(c, "fileA", 0, 100 * 1024)
	for i:= 0; i < 5; i ++ {
		s.checkLocalDownloadDataFromFileServer(c, "fileA", int64(i * 100 * 1024), 100*1024)
	}

	// test read fileB
	s.checkLocalDownloadDataFromFileServer(c, "fileB", 0, 1024 * 1024)
	s.checkLocalDownloadDataFromFileServer(c, "fileB", 0, 100 * 1024)
	for i:= 0; i < 20; i ++ {
		s.checkLocalDownloadDataFromFileServer(c, "fileB", int64(i * 50 * 1024), 50 * 1024)
	}
	s.checkLocalDownloadDataFromFileServer(c, "fileB", 1000 * 1024, 24 * 1024)

	// test read fileC
	s.checkLocalDownloadDataFromFileServer(c, "fileC", 0, 1500 * 1024)
	s.checkLocalDownloadDataFromFileServer(c, "fileC", 0, 100 * 1024)
	s.checkLocalDownloadDataFromFileServer(c, "fileC", 1400 * 1024, 100 * 1024)
	for i:= 0; i < 75; i ++ {
		s.checkLocalDownloadDataFromFileServer(c, "fileC", int64(i * 20 * 1024), 20 * 1024)
	}
}
