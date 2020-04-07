package seed

import (
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"io"
	"net/http"
	"os"
	"sync"
)

// cacheBuffer interface caches the seed file
type cacheBuffer interface {
	io.WriterAt
	// write close
	io.Closer
	Sync() error
	ReadStream(off int64, size int64) (io.ReadCloser, error)
	// remove the cache
	Remove() error

	// the cache full size
	FullSize() int64
}

func newFileCacheBuffer(path string, fullSize int64, trunc bool) (cb cacheBuffer, err error)  {
	var
	(
		fw    *os.File
	)

	_, err = os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil,  err
		}

		if !trunc {

		}
	}

	if trunc {
		fw, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY | os.O_TRUNC, 0644)
	}else{
		fw, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
	}

	if err != nil {
		return nil, err
	}

	fcb := &fileCacheBuffer{path: path, fw: fw, fullSize: fullSize}
	return fcb, nil
}

type fileCacheBuffer struct {
	sync.RWMutex
	prefetchOnce sync.Once

	path     		string
	fw       		*os.File
	remove  	 	bool
	fullSize     	int64
}

func (fcb *fileCacheBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	return fcb.fw.WriteAt(p, off)
}

func (fcb *fileCacheBuffer) Close() error {
	return fcb.fw.Close()
}

func (fcb *fileCacheBuffer) Sync() error {
	fcb.Lock()
	defer fcb.Unlock()

	if fcb.remove {
		return io.ErrClosedPipe
	}

	return fcb.fw.Sync()
}

func (fcb *fileCacheBuffer) ReadStream(off int64, size int64) (io.ReadCloser, error) {
	off, size, err := fcb.checkReadStreamParam(off, size)
	if err != nil {
		return nil, err
	}

	return fcb.openReadCloser(off, size)
}

func (fcb *fileCacheBuffer) Remove() error {
	fcb.Lock()
	defer fcb.Unlock()

	if fcb.remove {
		return nil
	}

	fcb.remove = true
	return os.Remove(fcb.path)
}

func (fcb *fileCacheBuffer) FullSize() int64 {
	fcb.RLock()
	defer fcb.RUnlock()

	return fcb.fullSize
}

func (fcb *fileCacheBuffer) checkReadStreamParam(off int64, size int64) (int64, int64, error) {
	fcb.RLock()
	defer fcb.RUnlock()

	if fcb.remove {
		return 0, 0, io.ErrClosedPipe
	}

	if off < 0 {
		off = 0
	}

	// if size <= 0, set range to [off, fullSize-1]
	if size <= 0 {
		size = fcb.fullSize - off
	}

	if off + size > fcb.fullSize {
		return 0, 0, errortypes.NewHttpError(http.StatusRequestedRangeNotSatisfiable, "out of range")
	}

	return off, size, nil
}

func (fcb *fileCacheBuffer) openReadCloser(off int64, size int64) (io.ReadCloser, error) {
	fr, err := os.Open(fcb.path)
	if err != nil {
		return nil, err
	}

	return newLimitReadCloser(fr, off, size)
}

type fileReadCloser struct {
	sr *io.SectionReader
	fr *os.File
}

func newLimitReadCloser(fr *os.File, off int64, size int64) (io.ReadCloser, error) {
	sr := io.NewSectionReader(fr, off, size)
	return &fileReadCloser{
		sr: sr,
		fr: fr,
	}, nil
}

func (lr *fileReadCloser) Read(p []byte) (n int, err error) {
	return lr.sr.Read(p)
}

func (lr *fileReadCloser) Close() error {
	return lr.fr.Close()
}
