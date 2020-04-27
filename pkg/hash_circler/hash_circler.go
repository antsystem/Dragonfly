package hash_circler

import (
	"fmt"
	"github.com/pkg/errors"
	"hash/fnv"
	"math"
	"sort"
	"sync"
)

// PreSetHashCircler hashes input string to target key, and the key could be enabled or disabled.
// And the keys array is preset, only the keys could be enable or disable.
type PreSetHashCircler interface {
	// Enable enables the target key in hash circle.
	Enable(key string) error

	// Hash hashes the input and output the target key which hashes.
	Hash(input string) (key string, err error)

	// Disable disables the target key
	Disable(key string)
}

var(
	ConflictErr     = errors.New("conflict key")
	TooManyRangeErr = errors.New("too many range")
	KeyNotPresetErr        = errors.New("key is not preset")
)

type circleRange struct {
	start   uint64
	end     uint64
	key     string
}

type keyStatus struct {
	key 	string
	enable  bool
}

type localPreSetHashCircler struct {
	sync.RWMutex
	validKeyCount int
	circleArr 	[]*circleRange
	keyArr      []*keyStatus
	hashFunc	func(string) uint64
}

func NewPreSetHashCircler(PreSetArr []string, hashFunc func(string)uint64) (PreSetHashCircler, error) {
	if hashFunc == nil {
		hashFunc = fnvHashFunc
	}

	if len(PreSetArr) == 0 {
		return nil, fmt.Errorf("taraget arr is nil")
	}

	keyArr := make([]*keyStatus, len(PreSetArr))
	circleArr := make([]*circleRange, len(PreSetArr))

	rangeSize := uint64(math.MaxUint64 / uint64(len(PreSetArr)))
	var start, end uint64

	for i, key := range PreSetArr {
		keyArr[i] = &keyStatus{
			key: key,
			enable: true,
		}

		end = start + rangeSize - 1
		circleArr[i] = &circleRange{
			key: key,
			start: start,
			end: end,
		}

		start = end + 1
	}

	circleArr[len(PreSetArr) - 1].end = math.MaxUint64 - 1

	return &localPreSetHashCircler{
		circleArr: circleArr,
		hashFunc: hashFunc,
		keyArr: keyArr,
		validKeyCount: len(circleArr),
	}, nil
}

func (h *localPreSetHashCircler) Enable(key string) error {
	h.Lock()
	defer h.Unlock()

	found := false
	index := 0

	for i, k := range h.keyArr {
		if k.key == key {
			index = i
			found = true
			if k.enable {
				return nil
			}

			k.enable = true
			index = i
			break
		}
	}

	if ! found {
		return KeyNotPresetErr
	}

	h.validKeyCount ++

	h.circleArr[index].key = key

	loopIndex := h.circleNextIndex(index, len(h.circleArr))
	for {
		if loopIndex == index {
			break
		}

		if h.keyArr[loopIndex].enable {
			break
		}

		h.circleArr[loopIndex].key = key
		loopIndex = h.circleNextIndex(loopIndex, len(h.circleArr))
	}

	return nil
}

func (h *localPreSetHashCircler) Hash(input string) (key string, err error) {
	h.RLock()
	defer h.RUnlock()

	if h.validKeyCount == 0 {
		return "", KeyNotPresetErr
	}

	hashIndex := h.hashFunc(input)
	rangeIndex := sort.Search(len(h.circleArr), func(i int) bool {
		if hashIndex <= h.circleArr[i].end {
			return true
		}

		return false
	})

	return h.circleArr[rangeIndex].key, nil
}

func (h *localPreSetHashCircler) Disable(key string) {
	h.Lock()
	defer h.Unlock()

	found := false
	index := 0

	for i, k := range h.keyArr {
		if k.key == key {
			index = i
			found = true
			if ! k.enable {
				return
			}

			k.enable = false
			index = i
			break
		}
	}

	if ! found {
		return
	}

	h.validKeyCount --
	if h.validKeyCount == 0 {
		return
	}

	newKey := h.circleArr[h.circlePrvIndex(index, len(h.circleArr))].key

	h.circleArr[index].key = newKey
	loopIndex := h.circleNextIndex(index, len(h.circleArr))
	for {
		if loopIndex == index {
			break
		}

		if h.keyArr[loopIndex].enable {
			break
		}

		h.circleArr[loopIndex].key = newKey
		loopIndex = h.circleNextIndex(loopIndex, len(h.circleArr))
	}

	return
}

func (h *localPreSetHashCircler) circlePrvIndex(index int, arrCount int) int {
	if index > 0 {
		return index - 1
	}

	return  arrCount - 1
}

func (h *localPreSetHashCircler) circleNextIndex(index int, arrCount int) int {
	if index < arrCount - 1 {
		return index + 1
	}

	return  0
}

func fnvHashFunc(input string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(input))
	return h.Sum64()
}
