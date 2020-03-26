package seed

import(
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"path/filepath"

	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"github.com/go-check/check"
)


func (s *SeedTestSuite) TestFileCacheBufferWithNoFile(c *check.C) {
	testDir := s.tmpDir

	cb, exist, err := newFileCacheBuffer(filepath.Join(testDir, "fileA"), 0, false)
	c.Assert(exist, check.Equals, false)
	c.Assert(err, check.IsNil)

	_, err = cb.ReadStream(0, -1)
	c.Assert(err, check.NotNil)

	data := []byte("0123456789")
	// write data
	n, err := io.Copy(cb, bytes.NewBuffer(data))
	c.Assert(int(n), check.Equals, len(data))
	c.Assert(err, check.IsNil)

	// write data
	_, err = cb.Seek(10, io.SeekStart)
	c.Assert(err, check.IsNil)
	n, err = io.Copy(cb, bytes.NewBuffer(data))
	c.Assert(int(n), check.Equals, len(data))
	c.Assert(err, check.IsNil)

	// read stream, expected failed
	_, err = cb.ReadStream(0, -1)
	c.Assert(err, check.NotNil)

	// write data
	_, err = cb.Seek(20, io.SeekStart)
	c.Assert(err, check.IsNil)
	n, err = io.Copy(cb, bytes.NewBuffer(data))
	c.Assert(int(n), check.Equals, len(data))
	c.Assert(err, check.IsNil)
	err = cb.Sync()
	c.Assert(err, check.IsNil)

	// close
	err = cb.Close()
	c.Assert(err, check.IsNil)

	// read all
	rc, err := cb.ReadStream(0, -1)
	c.Assert(err, check.IsNil)
	data1, err := ioutil.ReadAll(rc)
	c.Assert(err, check.IsNil)
	c.Assert(len(data1), check.Equals, len(data) * 3)
	expectAllData := []byte("012345678901234567890123456789")
	c.Assert(string(data1), check.Equals, string(expectAllData))
	err = rc.Close()
	c.Assert(err, check.IsNil)

	// read 10-
	rc, err = cb.ReadStream(10, -1)
	c.Assert(err, check.IsNil)
	data2, err := ioutil.ReadAll(rc)
	c.Assert(err, check.IsNil)
	c.Assert(len(data2), check.Equals, 20)
	expectData2 := []byte("01234567890123456789")
	c.Assert(string(data2), check.Equals, string(expectData2))
	err = rc.Close()
	c.Assert(err, check.IsNil)

	// read 5-14
	rc, err = cb.ReadStream(5, 10)
	c.Assert(err, check.IsNil)
	data3, err := ioutil.ReadAll(rc)
	c.Assert(err, check.IsNil)
	c.Assert(len(data3), check.Equals, 10)
	expectData3 := []byte("5678901234")
	c.Assert(string(data3), check.Equals, string(expectData3))
	err = rc.Close()
	c.Assert(err, check.IsNil)

	// read 20-30, expect failed
	_, err = cb.ReadStream(20, 11)
	httpErr, ok := err.(*errortypes.HttpError)
	c.Assert(ok, check.Equals, true)
	c.Assert(httpErr.HttpCode(), check.Equals, http.StatusRequestedRangeNotSatisfiable)

	// remove cache
	err = cb.Remove()
	c.Assert(err, check.IsNil)

	// read again
	_, err = cb.ReadStream(20, 5)
	c.Assert(err, check.NotNil)
}


func (s *SeedTestSuite) TestFileCacheBufferWithExistFile(c *check.C) {
	testDir := s.tmpDir

	// create cb
	cb, exist, err := newFileCacheBuffer(filepath.Join(testDir, "fileB"), 0, false)
	c.Assert(exist, check.Equals, false)
	c.Assert(err, check.IsNil)

	inputData1 := []byte("0123456789")
	inputData2 := []byte("abcde")

	// write data inputData1 * 3
	n, err := io.Copy(cb, bytes.NewBuffer(inputData1))
	c.Assert(int(n), check.Equals, len(inputData1))
	c.Assert(err, check.IsNil)

	n, err = io.Copy(cb, bytes.NewBuffer(inputData1))
	c.Assert(int(n), check.Equals, len(inputData1))
	c.Assert(err, check.IsNil)

	n, err = io.Copy(cb, bytes.NewBuffer(inputData1))
	c.Assert(int(n), check.Equals, len(inputData1))
	c.Assert(err, check.IsNil)

	err = cb.Close()
	c.Assert(err, check.IsNil)

	// reopen again
	cb, exist, err = newFileCacheBuffer(filepath.Join(testDir, "fileB"), 30, false)
	c.Assert(err, check.IsNil)
	c.Assert(exist, check.Equals, true)

	// write  data inputData2
	n, err = io.Copy(cb, bytes.NewBuffer(inputData2))
	c.Assert(int(n), check.Equals, len(inputData2))
	c.Assert(err, check.IsNil)

	// close
	err = cb.Close()
	c.Assert(err, check.IsNil)
	size, err := cb.Size()
	c.Assert(err, check.IsNil)
	c.Assert(int(size), check.Equals, 35)

	// read all
	rc, err := cb.ReadStream(0, -1)
	c.Assert(err, check.IsNil)
	data1, err := ioutil.ReadAll(rc)
	c.Assert(err, check.IsNil)
	c.Assert(len(data1), check.Equals, 35)
	expectAllData := []byte("012345678901234567890123456789abcde")
	c.Assert(string(data1), check.Equals, string(expectAllData))
	err = rc.Close()
	c.Assert(err, check.IsNil)

	// read 5-29
	rc, err = cb.ReadStream(5, 25)
	c.Assert(err, check.IsNil)
	data2, err := ioutil.ReadAll(rc)
	c.Assert(err, check.IsNil)
	c.Assert(len(data2), check.Equals, 25)
	expectData2 := []byte("5678901234567890123456789")
	c.Assert(string(data2), check.Equals, string(expectData2))
	err = rc.Close()
	c.Assert(err, check.IsNil)

	// read 10-34
	rc, err = cb.ReadStream(10, 25)
	c.Assert(err, check.IsNil)
	data3, err := ioutil.ReadAll(rc)
	c.Assert(err, check.IsNil)
	c.Assert(len(data3), check.Equals, 25)
	expectData3 := []byte("01234567890123456789abcde")
	c.Assert(string(data3), check.Equals, string(expectData3))
	err = rc.Close()
	c.Assert(err, check.IsNil)

	// read 10-35. expect failed
	_, err = cb.ReadStream(10, 26)
	httpErr, ok := err.(*errortypes.HttpError)
	c.Assert(ok, check.Equals, true)
	c.Assert(httpErr.HttpCode(), check.Equals, http.StatusRequestedRangeNotSatisfiable)

	// remove cache
	err = cb.Remove()
	c.Assert(err, check.IsNil)

	// read again
	_, err = cb.ReadStream(20, 5)
	c.Assert(err, check.NotNil)
}
