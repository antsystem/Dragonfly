package seed

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/go-check/check"

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

func (suite *SeedTestSuite) readFromFileServer(path string, off int64, size int64) ([]byte, error) {
	url := fmt.Sprintf("http://%s/%s", suite.host, path)
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

func (suite *SeedTestSuite) checkLocalDownloadDataFromFileServer(c *check.C, path string, off int64, size int64) {
	buf := newMockBufferWriterAt()

	ld := newLocalDownloader(fmt.Sprintf("http://%s/%s", suite.host, path), nil, ratelimiter.NewRateLimiter(0, 0), false)

	length, err := ld.DownloadToWriterAt(context.Background(), httputils.RangeStruct{StartIndex: off, EndIndex: off + size - 1}, 0, 0, buf, true)
	c.Check(err, check.IsNil)
	c.Check(size, check.Equals, length)

	expectData, err := suite.readFromFileServer(path, off, size)
	c.Check(err, check.IsNil)
	c.Check(string(buf.Bytes()), check.Equals, string(expectData))
}

func (suite *SeedTestSuite) TestLocalDownload(c *check.C) {
	// test read fileA
	suite.checkLocalDownloadDataFromFileServer(c, "fileA", 0, 500*1024)
	suite.checkLocalDownloadDataFromFileServer(c, "fileA", 0, 100*1024)
	for i := 0; i < 5; i++ {
		suite.checkLocalDownloadDataFromFileServer(c, "fileA", int64(i*100*1024), 100*1024)
	}

	// test read fileB
	suite.checkLocalDownloadDataFromFileServer(c, "fileB", 0, 1024*1024)
	suite.checkLocalDownloadDataFromFileServer(c, "fileB", 0, 100*1024)
	for i := 0; i < 20; i++ {
		suite.checkLocalDownloadDataFromFileServer(c, "fileB", int64(i*50*1024), 50*1024)
	}
	suite.checkLocalDownloadDataFromFileServer(c, "fileB", 1000*1024, 24*1024)

	// test read fileC
	suite.checkLocalDownloadDataFromFileServer(c, "fileC", 0, 1500*1024)
	suite.checkLocalDownloadDataFromFileServer(c, "fileC", 0, 100*1024)
	suite.checkLocalDownloadDataFromFileServer(c, "fileC", 1400*1024, 100*1024)
	for i := 0; i < 75; i++ {
		suite.checkLocalDownloadDataFromFileServer(c, "fileC", int64(i*20*1024), 20*1024)
	}

	//test read fileF
	for i := 0; i < 50; i++ {
		suite.checkLocalDownloadDataFromFileServer(c, "fileF", int64(i*20000), 20000)
	}
}
