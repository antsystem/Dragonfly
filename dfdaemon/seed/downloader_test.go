package seed

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/go-check/check"

	"github.com/dragonflyoss/Dragonfly/dfget/core/helper"
	"github.com/dragonflyoss/Dragonfly/pkg/httputils"
	"github.com/dragonflyoss/Dragonfly/pkg/ratelimiter"
)

type mockBufferWriterAt struct {
	buf *bytes.Buffer
}

func newMockBufferWriterAt() *mockBufferWriterAt {
	return &mockBufferWriterAt{
		buf: bytes.NewBuffer(nil),
	}
}

func (mb *mockBufferWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	if off != int64(mb.buf.Len()) {
		return 0, fmt.Errorf("failed to seek to %d", off)
	}

	return mb.buf.Write(p)
}

func (mb *mockBufferWriterAt) Bytes() []byte {
	return mb.buf.Bytes()
}

func (s *SeedTestSuite) SetUpSuite(c *check.C) {
	s.tmpDir = "./testdata"
	err := os.MkdirAll(s.tmpDir, 0774)
	c.Assert(err, check.IsNil)

	s.cacheDir = "./testcache"
	err = os.MkdirAll(s.cacheDir, 0774)
	c.Assert(err, check.IsNil)

	s.port = rand.Intn(1000) + 63000
	s.host = fmt.Sprintf("127.0.0.1:%d", s.port)

	s.server = helper.NewMockFileServer()
	err = s.server.StartServer(context.Background(), s.port)
	c.Assert(err, check.IsNil)

	// 500KB
	err = s.server.RegisterFile("fileA", 500*1024, "abcde0123456789")
	c.Assert(err, check.IsNil)
	// 1MB
	err = s.server.RegisterFile("fileB", 1024*1024, "abcdefg")
	c.Assert(err, check.IsNil)
	// 1.5 MB
	err = s.server.RegisterFile("fileC", 1500*1024, "abcdefg")
	c.Assert(err, check.IsNil)
	// 2 MB
	err = s.server.RegisterFile("fileD", 2048*1024, "abcdefg")
	c.Assert(err, check.IsNil)
	// 9.5 MB
	err = s.server.RegisterFile("fileE", 9500*1024, "abcdefg")
	c.Assert(err, check.IsNil)
	// 10 MB
	err = s.server.RegisterFile("fileF", 10*1024*1024, "abcdefg")
	c.Assert(err, check.IsNil)
	// 1 G
	err = s.server.RegisterFile("fileG", 1024*1024*1024, "1abcdefg")
	c.Assert(err, check.IsNil)

}

func (s *SeedTestSuite) TearDownSuite(c *check.C) {
	if s.tmpDir != "" {
		os.RemoveAll(s.tmpDir)
	}
	if s.cacheDir != "" {
		//os.RemoveAll(s.cacheDir)
	}
}

func (s *SeedTestSuite) readFromFileServer(path string, off int64, size int64) ([]byte, error) {
	url := fmt.Sprintf("http://%s/%s", s.host, path)
	header := map[string]string{}

	if size > 0 {
		header["Range"] = fmt.Sprintf("bytes=%d-%d", off, off+size-1)
	}

	code, data, err := httputils.GetWithHeaders(url, header, 5*time.Second)
	if err != nil {
		return nil, err
	}

	if code >= 400 {
		return nil, fmt.Errorf("resp code %d", code)
	}

	return data, nil
}

func (s *SeedTestSuite) checkLocalDownloadDataFromFileServer(c *check.C, path string, off int64, size int64) {
	buf := newMockBufferWriterAt()

	ld := newLocalDownloader(fmt.Sprintf("http://%s/%s", s.host, path), nil, ratelimiter.NewRateLimiter(0, 0))

	length, err := ld.DownloadToWriterAt(context.Background(), httputils.RangeStruct{StartIndex: off, EndIndex: off + size - 1}, 0, 0, buf, true)
	c.Check(err, check.IsNil)
	c.Check(size, check.Equals, length)

	expectData, err := s.readFromFileServer(path, off, size)
	c.Check(err, check.IsNil)
	c.Check(string(buf.Bytes()), check.Equals, string(expectData))

	// test with Download
	buffer := bytes.NewBuffer(nil)

	ld = newLocalDownloader(fmt.Sprintf("http://%s/%s", s.host, path), nil, ratelimiter.NewRateLimiter(0, 0))

	length, err = ld.Download(context.Background(), httputils.RangeStruct{StartIndex: off, EndIndex: off + size - 1}, 0, buffer)
	c.Check(err, check.IsNil)
	c.Check(size, check.Equals, length)

	expectData, err = s.readFromFileServer(path, off, size)
	c.Check(err, check.IsNil)
	c.Check(string(buffer.Bytes()), check.Equals, string(expectData))
}

func (s *SeedTestSuite) TestLocalDownload(c *check.C) {
	// test read fileA
	s.checkLocalDownloadDataFromFileServer(c, "fileA", 0, 500*1024)
	s.checkLocalDownloadDataFromFileServer(c, "fileA", 0, 100*1024)
	for i := 0; i < 5; i++ {
		s.checkLocalDownloadDataFromFileServer(c, "fileA", int64(i*100*1024), 100*1024)
	}

	// test read fileB
	s.checkLocalDownloadDataFromFileServer(c, "fileB", 0, 1024*1024)
	s.checkLocalDownloadDataFromFileServer(c, "fileB", 0, 100*1024)
	for i := 0; i < 20; i++ {
		s.checkLocalDownloadDataFromFileServer(c, "fileB", int64(i*50*1024), 50*1024)
	}
	s.checkLocalDownloadDataFromFileServer(c, "fileB", 1000*1024, 24*1024)

	// test read fileC
	s.checkLocalDownloadDataFromFileServer(c, "fileC", 0, 1500*1024)
	s.checkLocalDownloadDataFromFileServer(c, "fileC", 0, 100*1024)
	s.checkLocalDownloadDataFromFileServer(c, "fileC", 1400*1024, 100*1024)
	for i := 0; i < 75; i++ {
		s.checkLocalDownloadDataFromFileServer(c, "fileC", int64(i*20*1024), 20*1024)
	}

	//test read fileF
	for i := 0; i < 50; i++ {
		s.checkLocalDownloadDataFromFileServer(c, "fileF", int64(i*20000), 20000)
	}
}
