package scheduler

import (
	"math/rand"

	"github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"github.com/dragonflyoss/Dragonfly/pkg/stringutils"
	"github.com/dragonflyoss/Dragonfly/pkg/syncmap"

	"github.com/pkg/errors"
)

type taskStatePerNode struct {
	peerID string
	info   *types.TaskInfo
	path   string
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

func (ts *taskState) add(peerID string, path string, info *types.TaskInfo) error {
	if stringutils.IsEmptyStr(peerID) {
		return errors.Wrap(errortypes.ErrEmptyValue, "peerID")
	}

	_, err := ts.peerContainer.Get(peerID)
	if err != nil && !errortypes.IsDataNotFound(err) {
		return err
	}

	item := &taskStatePerNode{
		peerID: peerID,
		path:   path,
		info:   info,
	}

	return ts.peerContainer.Add(peerID, item)
}

// getPeersByLoad return the peers which satisfy the request, and order by load
// the number of peers should not more than maxCount;
func (ts *taskState) getPeersByLoad(maxCount int) []*taskStatePerNode {
	result := []*taskStatePerNode{}

	ts.peerContainer.Range(func(key, value interface{}) bool {
		pn := value.(*taskStatePerNode)
		result = append(result, pn)
		return true
	})

	rand.Shuffle(len(result), func(i, j int) {
		tmp := result[j]
		result[j] = result[i]
		result[i] = tmp
	})

	if maxCount > len(result) {
		return result
	}

	return result[:maxCount]
}
