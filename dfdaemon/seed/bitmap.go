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
}

func newBitMap(sizeOf64Bits int32, allSetBit bool) *bitmap {
	bm := make([]uint64, sizeOf64Bits)
	if allSetBit {
		for i := 0; i < int(sizeOf64Bits); i ++ {
			bm[i] = math.MaxUint64
		}
	}

	return &bitmap{
		bm:          bm,
		maxBitIndex: sizeOf64Bits * 64,
	}
}

// get gets the bits in range [start, end]. if set is true, return the set bits.
// else return the unset bits.
func (b *bitmap) get(start int32, end int32, setBit bool) []*bitmapRs {
	if end > b.maxBitIndex {
		end = b.maxBitIndex
	}

	if start < 0 {
		start = 0
	}

	b.RLock()
	defer  b.RUnlock()
	rs := []*bitmapRs{}
	var bit, lastBit uint64
	var bmp *bitmapRs
	// init lastBit to 2
	lastBit = 2

	for {
		if  start > end {
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
			start ++
			continue
		}

		if lastBit != bit {
			bmp = &bitmapRs{startIndex: start}
		}

		lastBit = bit
		start ++
	}

	if bmp != nil {
		bmp.endIndex = end
		rs = append(rs, bmp)
	}

	return rs
}

func (b *bitmap) set(start int32, end int32, setBit bool) {
	if end > b.maxBitIndex {
		end = b.maxBitIndex
	}

	if start < 0 {
		start = 0
	}

	b.Lock()
	defer  b.Unlock()

	second64MinIndex := ((start >> 6) + 1) << 6
	first64MaxIndex := end
	if first64MaxIndex >= second64MinIndex {
		first64MaxIndex = second64MinIndex - 1
	}

	for i:= start; i <= first64MaxIndex; i ++ {
		if setBit {
			b.bm[i>>6] = b.bm[i>>6] | lbm.Bit[i&63]
		}else{
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
		}else{
			b.bm[i>>6] = 0
		}
	}

	for i := last64MinIndex; i <= end; i ++ {
		if setBit {
			b.bm[i>>6] = b.bm[i>>6] | lbm.Bit[i&63]
		}else{
			b.bm[i>>6] = b.bm[i>>6] & (^lbm.Bit[i&63])
		}
	}
}

type bitmapRs struct {
	startIndex  int32
	endIndex    int32
}
