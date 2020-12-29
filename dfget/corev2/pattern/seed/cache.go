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
	"container/list"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/dragonflyoss/Dragonfly/pkg/util"
	"github.com/golang/groupcache/lru"
	"github.com/sirupsen/logrus"
)

type downloadFunc func(hdr map[string][]string, off, size int64) (int64, io.ReadCloser, error)
type downloadFuncWithContext func(ctx context.Context, hdr map[string][]string, off, size int64) (int64, io.ReadCloser, error)

const (
	maxEntries            = 16 * 1024
	maxSupportRequestSize = 128       // 128 * 512K = 64M
	lruSize               = 256
)

type memCacheEntry struct {
	sync.Mutex
	ready  bool
	data   []byte
	size   int64
	blkIdx int

	downloadFn func() (int64, io.ReadCloser, error)
}

func (e *memCacheEntry) Ready() bool {
	// if entry is ready, no possible to set not ready
	if e.ready {
		return e.ready
	}

	e.Lock()
	defer e.Unlock()

	return e.ready
}

func (e *memCacheEntry) Download() (err error) {
	e.Lock()
	defer e.Unlock()

	if e.ready {
		return nil
	}

	defer func() {
		if err != nil {
			errCounter.WithLabelValues("cache download failed").Inc()
		}
	}()

	dataLen, rc, err := e.downloadFn()
	if err != nil {
		return err
	}
	defer rc.Close()
	p := e.data
	totalBytes := int64(0)
	for {
		n, err := rc.Read(p)
		if n > 0 {
			totalBytes += int64(n)
		}
		if err == io.EOF || n == len(p) {
			break
		} else if err != nil {
			return err
		}
		p = p[n:]
	}
	if dataLen > 0 && totalBytes != dataLen {
		return fmt.Errorf("cache: data length err")
	}
	e.ready = true
	e.size = totalBytes

	return nil
}

type memCache struct {
	sync.RWMutex
	entries     []*list.List
	blockSize   int64
	availBlocks int64
	header      map[string][]string

	downloadFn downloadFunc
}

func NewMemCache(blockSize int64, f downloadFunc) *memCache {
	return &memCache{
		entries:     make([]*list.List, maxEntries),
		blockSize:   int64(blockSize),
		availBlocks: 0,
		downloadFn:  f,
	}
}

func (mc *memCache) getEntry(blkIdx int) *memCacheEntry {
	var (
		ll *list.List
		me *memCacheEntry
	)
	idx := blkIdx % maxEntries
	mc.RLock()
	if ll = mc.entries[idx]; ll != nil {
		// scan in list
		e := ll.Front()
		for e != nil {
			me, _ = e.Value.(*memCacheEntry)
			if me.blkIdx == blkIdx {
				mc.RUnlock()
				return me
			}
			e = e.Next()
		}
	}
	mc.RUnlock()
	mc.Lock()
	if mc.entries[idx] == nil {
		mc.entries[idx] = list.New()
	}
	ll = mc.entries[idx]
	me = &memCacheEntry{
		ready:  false,
		data:   make([]byte, mc.blockSize),
		size:   0,
		blkIdx: blkIdx,
		downloadFn: func() (int64, io.ReadCloser, error) {
			return mc.downloadFn(mc.header, int64(blkIdx)*mc.blockSize, mc.blockSize)
		},
	}
	ll.PushBack(me)
	cacheSize.WithLabelValues().Add(float64(mc.blockSize))
	mc.availBlocks++
	mc.Unlock()
	return me
}

func (mc *memCache) dropCache(idx int) {
	mc.Lock()
	defer mc.Unlock()

	mc.entries[idx] = nil
}

func (mc *memCache) updateHeader(hdr map[string][]string) {
	mc.Lock()
	defer mc.Unlock()
	mc.header = hdr
}

func (mc *memCache) AvailSize() int64 {
	return mc.availBlocks * mc.blockSize
}

func (mc *memCache) prepareBlocks(startIdx, endIdx int) {
	idx := startIdx
	// download all blocks if need
	for idx <= endIdx {
		e := mc.getEntry(idx)
		if !e.Ready() {
			go e.Download()
		}
		idx++
	}
}

func (mc *memCache) NewReadCloser(off, size int64) (int64, io.ReadCloser, error) {
	startIdx := int(off / mc.blockSize)
	endIdx := int((off + size - 1) / mc.blockSize)
	if endIdx-startIdx+1 > maxSupportRequestSize {
		errCounter.WithLabelValues("too large request").Inc()
		return 0, nil, fmt.Errorf("too large request")
	}
	mc.prepareBlocks(startIdx, endIdx)
	rds := make([]*bytes.Reader, endIdx-startIdx+1)
	// check all blocks ready
	idx := startIdx
	dataLen := int64(0)
	for idx <= endIdx {
		e := mc.getEntry(idx)
		si := util.Max(0, off-int64(idx)*mc.blockSize)
		hit := true
		if !e.Ready() {
			hit = false
			err := e.Download()
			if err != nil {
				logrus.Errorf("cache:download err %v", err)
				mc.dropCache(idx)
				return -1, nil, err
			}
		}
		ei := util.Min(e.size, off+size-int64(idx)*mc.blockSize)
		if hit {
			recordHitCacheFlowCounter(ei - si)
		}
		rds[idx-startIdx] = bytes.NewReader(e.data[si:ei])
		idx++
		dataLen += ei - si
	}
	return dataLen, &memCacheReadCloser{rds: rds}, nil
}

type memCacheReadCloser struct {
	rds []*bytes.Reader
	idx int
}

func (r *memCacheReadCloser) Close() error {
	return nil
}

func (r *memCacheReadCloser) Read(p []byte) (int, error) {
	if r.idx >= len(r.rds) {
		return 0, io.EOF
	}
	readLen := 0
	for r.idx < len(r.rds) {
		rd := r.rds[r.idx]
		n, _ := rd.Read(p)
		if rd.Len() == 0 {
			r.idx++
		}
		readLen += n
		if n == len(p) {
			break
		}
		p = p[n:]
	}

	return readLen, nil
}

func (r *memCacheReadCloser) WriteTo(w io.Writer) (int64, error) {
	writeLen := int64(0)
	for r.idx < len(r.rds) {
		n, err := r.rds[r.idx].WriteTo(w)
		if err != nil {
			return writeLen + n, err
		}
		r.idx++
		writeLen += n
	}

	return writeLen, nil
}

type downloadCacheManager struct {
	sync.Mutex
	waterMark int64
	size      int64
	blockSize int64
	cache     *lru.Cache
	ctx       context.Context
}

func NewDownloadCacheManager(ctx context.Context, waterMark int64, blockSize int64) *downloadCacheManager {
	dm := &downloadCacheManager{
		waterMark: waterMark,
		size:      0,
		blockSize: blockSize,
		cache:     lru.New(lruSize),
		ctx:       ctx,
	}
	dm.cache.OnEvicted = func(k lru.Key, v interface{}) {
		mc, _ := v.(*memCache)
		dm.dec(mc.AvailSize())
		logrus.Infof("memCache remove %v", k)
		cacheCounter.WithLabelValues().Dec()
		cacheSize.WithLabelValues().Sub(float64(mc.AvailSize()))
	}

	return dm
}

func (dm *downloadCacheManager) inc(size int64) {
	dm.size += size
}

func (dm *downloadCacheManager) dec(size int64) {
	dm.size -= size
}

func (dm *downloadCacheManager) clean() {
	dm.cache.RemoveOldest()
}

func (dm *downloadCacheManager) Gc() {
	for {
		dm.Lock()
		if dm.underWater() {
			dm.Unlock()
			break
		}
		dm.clean()
		dm.Unlock()
	}
}

func (dm *downloadCacheManager) getCache(url string, hdr map[string][]string, f downloadFuncWithContext) *memCache {
	dm.Lock()
	defer dm.Unlock()

	var mc *memCache
	if !dm.underWater() && dm.cache.Len() == 1 {
		return nil
	}
	if v, ok := dm.cache.Get(url); ok {
		mc, _ = v.(*memCache)
		mc.updateHeader(hdr)
		return mc
	}
	mc = NewMemCache(dm.blockSize, func(hdr map[string][]string, off, size int64) (int64, io.ReadCloser, error) {
		dataLen, rc, err := f(dm.ctx, hdr, off, size)
		if err == nil {
			dm.inc(size)
		}
		return dataLen, rc, err
	})
	dm.cache.Add(url, mc)
	cacheCounter.WithLabelValues().Inc()
	mc.updateHeader(hdr)
	return mc
}

func (dm *downloadCacheManager) StreamContext(url string, hdr map[string][]string, off, size int64, f downloadFuncWithContext) (int64, io.ReadCloser, error) {
	mc := dm.getCache(url, hdr, f)
	if mc == nil {
		return -1, nil, fmt.Errorf("cache full")
	}
	dataLen, rc, err := mc.NewReadCloser(off, size)
	if err != nil {
		return -1, nil, err
	}
	if !dm.underWater() {
		go dm.Gc()
	}
	return dataLen, rc, nil
}

func (dm *downloadCacheManager) cacheCount() int {
	return dm.cache.Len()
}

func (dm *downloadCacheManager) underWater() bool {
	return dm.size <= dm.waterMark
}
