package scheduler

import (
	"sort"

	"github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly/pkg/stringutils"
	"github.com/dragonflyoss/Dragonfly/pkg/syncmap"

	"github.com/pkg/errors"
)

type taskStatePerNode struct {
	peerID string
	load   *int64
	info   *types.TaskInfo
	path   string
}

type loadSorter struct {
	items []*taskStatePerNode
}

func (ls *loadSorter) Len() int {
	return len(ls.items)
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (ls *loadSorter) Less(i, j int) bool {
	return *(ls.items[i].load) < *(ls.items[j].load)
}

// Swap swaps the elements with indexes i and j.
func (ls *loadSorter) Swap(i, j int) {
	tmp := ls.items[i]
	ls.items[i] = ls.items[j]
	ls.items[j] = tmp
}

type taskState struct {
	// key is peerID, value is taskStatePerNode
	peerContainer *syncmap.SyncMap
}

func newTaskState() *taskState {
	return &taskState{
		peerContainer: syncmap.NewSyncMap(),
	}
}

func (ts *taskState) add(peerID string, load *int64, path string, info *types.TaskInfo) error {
	if stringutils.IsEmptyStr(peerID) {
		return errors.Wrap(errortypes.ErrEmptyValue, "peerID")
	}

	_, err := ts.peerContainer.Get(peerID)
	if err != nil && !errortypes.IsDataNotFound(err) {
		return err
	}

	item := &taskStatePerNode{
		peerID: peerID,
		load:   load,
		path:   path,
		info:   info,
	}

	return ts.peerContainer.Add(peerID, item)
}

// getPeersByLoad return the peers which satisfy the request, and order by load
// the number of peers should not more than maxCount;
// the peer load should not more than maxLoad.
func (ts *taskState) getPeersByLoad(maxCount int) []*taskStatePerNode {
	sorter := loadSorter{}

	ts.peerContainer.Range(func(key, value interface{}) bool {
		pn := value.(*taskStatePerNode)
		sorter.items = append(sorter.items, pn)
		return true
	})

	sort.Sort(&sorter)
	if maxCount > len(sorter.items) {
		return sorter.items
	}

	return sorter.items[:maxCount]
}
