package seed

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"

	lbm "github.com/openacid/low/bitmap"

	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
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

func newFileCacheBuffer(path string, fullSize int64, trunc bool, memoryCache bool, blockOrder uint32) (cb cacheBuffer, err error)  {
	var
	(
		fw    *os.File
	)

	_, err = os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil,  err
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

	fcb := &fileCacheBuffer{path: path, fw: fw, fullSize: fullSize, memoryCache: memoryCache}
	if memoryCache {
		fcb.blockOrder = blockOrder
		fcb.blockSize = 1 << blockOrder
		blocks := fullSize/int64(fcb.blockSize)
		if (fullSize % int64(fcb.blockSize)) > 0 {
			blocks ++
		}

		sizeOf64Bits := blocks/64
		if (blocks % 64) > 0 {
			sizeOf64Bits ++
		}

		fcb.blockMeta = newBitMap(int32(sizeOf64Bits), false)
		fcb.memCacheMap = make(map[int64]*bytes.Buffer)
	}

	return fcb, nil
}

type fileCacheBuffer struct {
	sync.RWMutex

	path     		string
	fw       		*os.File
	remove  	 	bool
	fullSize     	int64

	// memory cache mode, in the mode, it will store cache in temperate memory.
	// if sync is called, the temperate memory will sync to local fs.
	// in memory cache mode, WriteAt should transfer a block buffer.
	memoryCache		bool
	blockMeta		*bitmap
	blockSize		int32
	blockOrder		uint32
	// the key is bytes start index of block
	memCacheMap     map[int64]*bytes.Buffer
}

// if in memory mode, the write data buffer will be reused, so don't change the buffer.
func (fcb *fileCacheBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	err = fcb.checkWriteAtValid(off, int64(len(p)))
	if err != nil {
		return
	}

	if fcb.memoryCache {
		fcb.storeMemoryCache(p, off)
		return len(p), nil
	}

	return fcb.fw.WriteAt(p, off)
}

func (fcb *fileCacheBuffer) Close() error {
	if err := fcb.Sync(); err != nil {
		return err
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

	if fcb.memoryCache {
		if err := fcb.syncMemoryCache(); err != nil {
			return err
		}
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

func (fcb *fileCacheBuffer) storeMemoryCache(p []byte, off int64) {
	fcb.Lock()
	defer fcb.Unlock()

	if _, ok := fcb.memCacheMap[off]; ok {
		return
	}

	buf := bytes.NewBuffer(p)
	fcb.memCacheMap[off] = buf

	startBlockIndex := int32(off >> fcb.blockOrder)
	// set bits in bit map
	fcb.blockMeta.set(startBlockIndex, startBlockIndex, true)
}

func (fcb *fileCacheBuffer) syncMemoryCache() error {
	var(
		err error
	)

	offArr := []int64{}
	bufArr := []*bytes.Buffer{}

	fcb.RLock()
	for off, buf := range fcb.memCacheMap {
		offArr = append(offArr, off)
		bufArr = append(bufArr, buf)
	}
	fcb.RUnlock()

	for i := 0; i < len(offArr); i ++ {
		err = fcb.syncBlockToFile(bufArr[i].Bytes(), offArr[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func (fcb *fileCacheBuffer) syncBlockToFile(p []byte, off int64) error {
	n, err := fcb.fw.WriteAt(p, off)
	if err != nil {
		return err
	}

	if n < len(p) {
		return io.ErrShortWrite
	}

	fcb.Lock()
	defer fcb.Unlock()

	blockIndex := int32(off >> fcb.blockOrder)
	delete(fcb.memCacheMap, off)
	fcb.blockMeta.set(blockIndex, blockIndex, false)
	return nil
}

func (fcb *fileCacheBuffer) openReadCloser(off int64, size int64) (io.ReadCloser, error) {
	if fcb.memoryCache {
		return fcb.openReadCloserInMemoryCacheMode(off, size)
	}

	fr, err := os.Open(fcb.path)
	if err != nil {
		return nil, err
	}

	return newLimitReadCloser(fr, off, size)
}


// if in memory cache mode, the reader is multi reader in which some data in memory and others in file.
func (fcb *fileCacheBuffer) openReadCloserInMemoryCacheMode(off , size int64) (io.ReadCloser, error) {
	var(
		rds []io.Reader
		useFr  bool
	)

	fr, err := os.Open(fcb.path)
	if err != nil {
		return nil, err
	}

	fcb.RLock()
	defer fcb.RUnlock()

	if len(fcb.memCacheMap) == 0 {
		return newLimitReadCloser(fr, off, size)
	}

	currentOff := off
	var currentBlock int32
	var currentBlockStartBytes, currentBlockEndBytes, useBlockBytes, currentBlockOff, currentBlockOffEnd int64
	for{
		if size <= 0 {
			break
		}

		currentBlock = int32(currentOff >> fcb.blockOrder)
		currentBlockStartBytes = int64(currentBlock) << fcb.blockOrder
		currentBlockEndBytes = currentBlockStartBytes + int64(fcb.blockSize) - 1
		if  currentBlockEndBytes >= fcb.fullSize {
			currentBlockEndBytes = fcb.fullSize - 1
		}

		useBlockBytes = currentBlockEndBytes - currentOff + 1
		if useBlockBytes > size {
			useBlockBytes = size
		}

		currentBlockOff = currentOff - currentBlockStartBytes
		currentBlockOffEnd = currentBlockOff + useBlockBytes - 1
		buf, ok := fcb.memCacheMap[currentBlockStartBytes]
		if ok {
			// currentBlock in memory
			b := buf.Bytes()
			rd := bytes.NewReader(b[currentBlockOff: currentBlockOffEnd + 1])
			rds = append(rds, rd)
		}else{
			// else currentBlock in file
			rd := io.NewSectionReader(fr, currentOff, useBlockBytes)
			rds = append(rds, rd)
			useFr = true
		}

		size -= useBlockBytes
		currentOff += useBlockBytes
	}

	if !useFr {
		fr.Close()
		fr = nil
	}

	return newMultiReadCloser(rds, fr), nil
}

func (fcb *fileCacheBuffer) checkWriteAtValid(off, size int64) error {
	if !fcb.memoryCache {
		return nil
	}

	if uint64(off) & (lbm.Mask[fcb.blockOrder]) > 0 {
		return fmt.Errorf("memory cache mode, off %d should be align with blockSize %d", off, fcb.blockSize)
	}

	maxIndex := off + size - 1

	if  maxIndex >= fcb.fullSize {
		return fmt.Errorf("memory cache mode, max write index %d should be smaller than max block index %d", maxIndex, fcb.fullSize)
	}

	// if last block, the size may smaller than block size
	if int32(off >> fcb.blockOrder) == fcb.blockMeta.maxBitIndex {
		return nil
	}

	if size != int64(fcb.blockSize) {
		return fmt.Errorf("memory cache mode, len of bytes %d should be equal to block size %d", size, fcb.blockSize)
	}

	return nil
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

type multiReadCloser struct {
	rds 	[]io.Reader
	realRd  io.Reader
	fr		*os.File
}

func newMultiReadCloser(rds []io.Reader, fr *os.File) *multiReadCloser {
	return &multiReadCloser{
		rds: rds,
		realRd: io.MultiReader(rds...),
		fr: fr,
	}
}

func (mr *multiReadCloser) Read(p []byte) (n int, err error) {
	return mr.realRd.Read(p)
}

func (mr *multiReadCloser) Close() error {
	if mr.fr != nil {
		return mr.fr.Close()
	}

	return nil
}

