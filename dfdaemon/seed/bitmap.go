package seed

import (
	"math"
	"sync"

	lbm "github.com/openacid/low/bitmap"
)

type bitmap struct {
	sync.RWMutex
	bm          []uint64
	maxBitIndex int32

	lockMap map[int32]*lockInfo
}

func newBitMap(sizeOf64Bits int32, allSetBit bool) *bitmap {
	bm := make([]uint64, sizeOf64Bits)
	if allSetBit {
		for i := 0; i < int(sizeOf64Bits); i++ {
			bm[i] = math.MaxUint64
		}
	}

	return &bitmap{
		bm:          bm,
		maxBitIndex: sizeOf64Bits * 64,
		lockMap:     make(map[int32]*lockInfo),
	}
}

// get gets the bits in range [start, end]. if set is true, return the set bits.
// else return the unset bits.
func (b *bitmap) get(start int32, end int32, setBit bool) []*bitmapRs {
	b.RLock()
	defer b.RUnlock()

	return b.getWithoutLock(start, end, setBit)
}

// set sets or cleans the bits in range [start, end]. if setBit is true, set bits. else clean bits.
func (b *bitmap) set(start int32, end int32, setBit bool) {
	b.Lock()
	defer b.Unlock()

	b.setWithoutLock(start, end, setBit)
}

// lockRange lock the range [start, end], it will set bits in bitmap.
// if part of range has been set bits, it will return the unset bits range.
func (b *bitmap) lockRange(start int32, end int32) *lockInfoRs {
	b.Lock()
	defer b.Unlock()

	if start < 0 {
		start = 0
	}

	if end > b.maxBitIndex {
		end = b.maxBitIndex
	}

	rs := &lockInfoRs{}
	waitChs := []chan struct{}{}

	// get the unset bits range
	unsetRange := b.getWithoutLock(start, end, false)
	rs.unsetRange = unsetRange

	// store to lock map to lock range
	for _, s := range unsetRange {
		b.lockMap[s.startIndex] = &lockInfo{
			start:  s.startIndex,
			end:    s.endIndex,
			notify: make(chan struct{}),
		}
	}

	// get set bits range to fetch the notify chan
	setRange := b.getWithoutLock(start, end, true)
	if len(setRange) == 0 {
		goto end
	}

	// if the first set bits is start, it means start index of its lock range may be smaller than start.
	if setRange[0].startIndex == start {
		if _, ok := b.lockMap[start]; ok {
			goto fetchWaitCh
		}

		for i := start - 1; i >= 0; i-- {
			if info, ok := b.lockMap[i]; ok {
				waitChs = append(waitChs, info.notify)
				setRange[0].startIndex = info.end + 1
				break
			}
		}
	}

fetchWaitCh:
	for i := 0; i < len(setRange); i++ {
		lockKey := setRange[i].startIndex
		for {
			if lockKey > setRange[i].endIndex {
				break
			}

			info, ok := b.lockMap[lockKey]
			if !ok {
				// warning: code should not goto the branch
				break
			}
			waitChs = append(waitChs, info.notify)
			lockKey = info.end + 1
		}
	}

	rs.waitChs = waitChs

end:
	// set bits in the range [start, end]
	b.setWithoutLock(start, end, true)
	return rs
}

// unlockRange remove the range from lockMap and close the notify channel.
func (b *bitmap) unlockRange(start int32, end int32) {
	b.Lock()
	defer b.Unlock()

	b.setWithoutLock(start, end, false)

	info, ok := b.lockMap[start]
	if !ok {
		return
	}

	close(info.notify)
	delete(b.lockMap, start)
}

func (b *bitmap) getWithoutLock(start int32, end int32, setBit bool) []*bitmapRs {
	if end > b.maxBitIndex {
		end = b.maxBitIndex
	}

	if start < 0 {
		start = 0
	}

	rs := []*bitmapRs{}
	var bit, lastBit uint64
	var bmp *bitmapRs
	// init lastBit to 2
	lastBit = 2

	for {
		if start > end {
			break
		}

		bit = lbm.Get1(b.bm, start)
		if setBit != (bit == 1) {
			if bmp != nil {
				bmp.endIndex = start - 1
				rs = append(rs, bmp)
				bmp = nil
			}

			lastBit = bit
			start++
			continue
		}

		if lastBit != bit {
			bmp = &bitmapRs{startIndex: start}
		}

		lastBit = bit
		start++
	}

	if bmp != nil {
		bmp.endIndex = end
		rs = append(rs, bmp)
	}

	return rs
}

func (b *bitmap) setWithoutLock(start int32, end int32, setBit bool) {
	if end > b.maxBitIndex {
		end = b.maxBitIndex
	}

	if start < 0 {
		start = 0
	}

	second64MinIndex := ((start >> 6) + 1) << 6
	first64MaxIndex := end
	if first64MaxIndex >= second64MinIndex {
		first64MaxIndex = second64MinIndex - 1
	}

	for i := start; i <= first64MaxIndex; i++ {
		if setBit {
			b.bm[i>>6] = b.bm[i>>6] | lbm.Bit[i&63]
		} else {
			b.bm[i>>6] = b.bm[i>>6] & (^lbm.Bit[i&63])
		}
	}

	last64MinIndex := (end >> 6) << 6
	if last64MinIndex < first64MaxIndex {
		last64MinIndex = first64MaxIndex + 1
	}

	for i := second64MinIndex; i < last64MinIndex; i += 64 {
		if setBit {
			b.bm[i>>6] = math.MaxUint64
		} else {
			b.bm[i>>6] = 0
		}
	}

	for i := last64MinIndex; i <= end; i++ {
		if setBit {
			b.bm[i>>6] = b.bm[i>>6] | lbm.Bit[i&63]
		} else {
			b.bm[i>>6] = b.bm[i>>6] & (^lbm.Bit[i&63])
		}
	}
}

type bitmapRs struct {
	startIndex int32
	endIndex   int32
}

type lockInfo struct {
	start  int32
	end    int32
	notify chan struct{}
}

type lockInfoRs struct {
	// rs stores  the range which is locked by caller
	unsetRange []*bitmapRs
	//
	waitChs []chan struct{}
}
