/*
 * Copyright The Dragonfly Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package seed

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"sync"
	"syscall"

	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"github.com/sirupsen/logrus"
)

// cacheBuffer interface caches the seed file
type cacheBuffer interface {
	ReadAtFrom
	io.WriterAt
	// write close
	io.Closer
	Sync() error

	// ReadStream prepares io.ReadCloser from cacheBuffer.
	ReadStream(off int64, size int64) (io.ReadCloser, error)

	// remove the cache
	Remove() error

	// the cache full size
	FullSize() int64
}

func newFileCacheBuffer(path string, fullSize int64, trunc bool, memoryCache bool) (cb cacheBuffer, err error) {
	var (
		fw *os.File
	)

	_, err = os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	}

	if trunc {
		fw, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	} else {
		fw, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	}
	if err != nil {
		return nil, err
	}

	err = fw.Truncate(fullSize)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			fw.Close()
		}
	}()

	fcb := &fileCacheBuffer{path: path, fw: fw, fullSize: fullSize, useMemoryCache: memoryCache}
	if memoryCache {
		fcb.memCache, err = syscall.Mmap(int(fw.Fd()), 0, int(fullSize),
			syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
		if err != nil {
			logrus.Errorf("mmap resource %s err", path)
			fcb.useMemoryCache = false
		}
	}

	return fcb, nil
}

type fileCacheBuffer struct {
	// the lock protects fields of 'remove', 'memCacheMap'
	sync.RWMutex

	path     string
	fw       *os.File
	remove   bool
	fullSize int64

	// memory cache mode, in the mode, it will store cache in page cache with syscall.Mmap
	useMemoryCache bool
	memCache       []byte
}

type bytesReadCloser struct {
	*bytes.Reader
}

func (r *bytesReadCloser) Close() error {
	return nil
}

// if in memory mode, the write data buffer will be reused, so don't change the buffer.
func (fcb *fileCacheBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	if fcb.useMemoryCache {
		n = copy(fcb.memCache[off:], p)
		return n, nil
	}

	return fcb.fw.WriteAt(p, off)
}

func (fcb *fileCacheBuffer) ReadAtFrom(r io.Reader, off, size int64) (n int, err error) {
	if fcb.useMemoryCache && fcb.memCache != nil {
		if off+size > fcb.fullSize {
			size = fcb.fullSize - off
		}
		return io.ReadFull(r, fcb.memCache[off:off+size])
	}
	// fallback to writeAt
	written, err := CopyBufferToWriterAt(off, fcb, r)
	return int(written), err
}

// Close closes the file writer.
func (fcb *fileCacheBuffer) Close() error {
	if err := fcb.Sync(); err != nil {
		return err
	}
	if fcb.memCache != nil {
		syscall.Munmap(fcb.memCache)
	}
	return fcb.fw.Close()
}

func (fcb *fileCacheBuffer) Sync() error {
	fcb.Lock()
	remove := fcb.remove
	fcb.Unlock()

	if remove {
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

	// Note: if file size if zero, it should be specially handled in the upper caller.
	// In current progress, if size <= 0, it means to read to the end of file.
	// if size <= 0, set range to [off, fullSize-1]
	if size <= 0 {
		size = fcb.fullSize - off
	}

	if off+size > fcb.fullSize {
		return 0, 0, errortypes.NewHTTPError(http.StatusRequestedRangeNotSatisfiable, "out of range")
	}

	return off, size, nil
}

func (fcb *fileCacheBuffer) openReadCloser(off int64, size int64) (io.ReadCloser, error) {
	if fcb.useMemoryCache && fcb.memCache != nil {
		return &bytesReadCloser{
			Reader: bytes.NewReader(fcb.memCache[off:off+size]),
		}, nil
	}

	fr, err := os.Open(fcb.path)
	if err != nil {
		return nil, err
	}

	return newLimitReadCloser(fr, off, size)
}

// fileReadCloser provides a selection readCloser of file.
type fileReadCloser struct {
	sr *io.SectionReader
	fr *os.File
}

func newLimitReadCloser(fr *os.File, off int64, size int64) (io.ReadCloser, error) {
	sr := io.NewSectionReader(fr, off, size)
	fr.Seek(off, io.SeekStart)
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

type writeOnly struct {
	io.Writer
}

func (lr *fileReadCloser) WriteTo(w io.Writer) (int64, error) {
	if rf, ok := w.(io.ReaderFrom); ok {
		return rf.ReadFrom(io.LimitReader(lr.fr, lr.sr.Size()))
	}
	// hardly run here
	buf := make([]byte, 64*1024)
	return io.CopyBuffer(writeOnly{w}, lr.sr, buf)
}
