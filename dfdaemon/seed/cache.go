package seed

import (
	"io"
	"net/http"
	"os"
	"sync"

	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
)

// cacheBuffer interface caches the seed file
type cacheBuffer interface {
	io.WriterAt
	io.Closer
	// lock the range [0, size-1], it don't allow to alter the range.
	LockSize(int64)
	Sync() error
	ReadStream(off int64, size int64) (io.ReadCloser, error)
	// remove the cache
	Remove() error

	// cache size
	Size() int64
}

// if file size is shorter than existSize, exist return false and write from 0;
// if file size is longer or equal than existSize, exist return true and write from existSize.
// if finished set true and file Size is longer or equal than existSize, exist return true. It will
// no need to write again.
func newFileCacheBuffer(path string, existSize int64, finished bool) (cb cacheBuffer, exist bool, err error) {
	var (
		fw    *os.File
		trunc bool = true
	)

	if existSize > 0 {
		info, err := os.Stat(path)
		if err != nil {
			if !os.IsNotExist(err) {
				return nil, false, err
			}

			goto truncWrite
		}

		if info.Size() < existSize {
			goto truncWrite
		}

		trunc = false
		exist = true
		fw, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, false, err
		}

		fw.Seek(existSize, io.SeekStart)
	}

truncWrite:
	if trunc {
		fw, err = os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, false, err
		}
	}

	fcb := &fileCacheBuffer{path: path, fw: fw, size: existSize, lockSize: existSize}
	if finished && exist {
		fcb.finished = finished
	}

	return fcb, exist, nil
}

type fileCacheBuffer struct {
	sync.RWMutex

	path     string
	fw       *os.File
	finished bool
	remove   bool
	size     int64
	lockSize int64
}

func (fcb *fileCacheBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	fcb.Lock()
	if fcb.finished || fcb.remove {
		//todo: return the error
		fcb.Unlock()
		return 0, io.ErrClosedPipe
	}
	fcb.size = off
	fcb.Unlock()

	n, err = fcb.fw.WriteAt(p, off)

	fcb.Lock()
	defer fcb.Unlock()

	fcb.size = off + int64(n)
	return n, err
}

func (fcb *fileCacheBuffer) Close() error {
	fcb.Lock()
	defer fcb.Unlock()

	if fcb.remove {
		return io.ErrClosedPipe
	}

	if fcb.finished {
		return nil
	}

	err := fcb.fw.Close()
	if err != nil {
		return err
	}

	// get the size of file
	fi, err := os.Stat(fcb.path)
	if err != nil {
		return err
	}

	fcb.size = fi.Size()
	fcb.lockSize = fcb.size
	fcb.finished = true
	return nil
}

func (fcb *fileCacheBuffer) Sync() error {
	fcb.Lock()
	defer fcb.Unlock()

	if fcb.remove {
		return io.ErrClosedPipe
	}

	if fcb.finished {
		return nil
	}

	return fcb.fw.Sync()
}

func (fcb *fileCacheBuffer) ReadStream(off int64, size int64) (io.ReadCloser, error) {
	fcb.RLock()
	defer fcb.RUnlock()

	if fcb.remove {
		return nil, io.ErrClosedPipe
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

func (fcb *fileCacheBuffer) Size() int64 {
	fcb.RLock()
	defer fcb.RUnlock()

	return fcb.size
}

func (fcb *fileCacheBuffer) LockSize(size int64) {
	fcb.Lock()
	defer fcb.Unlock()

	fcb.lockSize = size
}

func (fcb *fileCacheBuffer) openReadCloser(off int64, size int64) (io.ReadCloser, error) {
	if off < 0 {
		off = 0
	}

	// if size <= 0, set range to [off, lockSize-1]
	if size <= 0 {
		size = fcb.lockSize - off
	}

	if off + size > fcb.lockSize {
		return nil, errortypes.NewHttpError(http.StatusRequestedRangeNotSatisfiable, "out of range")
	}

	fr, err := os.Open(fcb.path)
	if err != nil {
		return nil, err
	}

	return newLimitReadCloser(fr, off, size)
}

type limitReadCloser struct {
	sr *io.SectionReader
	fr *os.File
}

func newLimitReadCloser(fr *os.File, off int64, size int64) (io.ReadCloser, error) {
	sr := io.NewSectionReader(fr, off, size)
	return &limitReadCloser{
		sr: sr,
		fr: fr,
	}, nil
}

func (lr *limitReadCloser) Read(p []byte) (n int, err error) {
	return lr.sr.Read(p)
}

func (lr *limitReadCloser) Close() error {
	return lr.fr.Close()
}
