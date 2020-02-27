package scheduler

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"
	"sync"

	"github.com/sirupsen/logrus"
)

const(
	defaultMaxLoad = 5
	defaultMaxPeers = 3
)

// SchedulerManager schedules the peer node to fetch the resource specified by request
type SchedulerManager struct {
	sync.Mutex

	// generation
	generation     int64

	// key is taskID, value is taskState
	taskContainer  *dataMap

	// key is peerID, value is Node
	nodeContainer  *dataMap

	// key is the taskURL
	urlContainer  *dataMap


	downloadStartCh  chan notifySt
	downloadFinishCh chan notifySt
}

func NewScheduler() *SchedulerManager {
	sm := &SchedulerManager{
		taskContainer: newDataMap(),
		downloadStartCh: make(chan notifySt, 10),
		downloadFinishCh: make(chan notifySt, 10),
	}

	go sm.adjustPeerLoad()
	return sm
}

// todo:
func (sm *SchedulerManager) adjustPeerLoad() {
	for{
		select {
		case st := <- sm.downloadStartCh:
			if st.generation != sm.generation {
				logrus.Infof("in local schedule downloadStartCh, notify %v is out of generation", st)
				continue
			}

			sm.updateNodeLoad(st.peerID, 1)

		case st := <- sm.downloadFinishCh:
			if st.generation != sm.generation {
				logrus.Infof("in local schedule downloadFinishCh , notify %v is out of generation", st)
				continue
			}

			sm.updateNodeLoad(st.peerID, -1)
		}
	}
}

// if pieceRange == "" means all Pieces of file
func (sm *SchedulerManager) SchedulerByTaskID(ctx context.Context, taskID string, srcCid string, pieceRange string, pieceSize int32) ([]*Result, error) {
	sm.Lock()
	defer sm.Unlock()

	state, err := sm.taskContainer.getAsTaskState(taskID)
	if err != nil {
		return nil, err
	}

	pns := state.getPeersByLoad(defaultMaxPeers, defaultMaxLoad)
	if len(pns) == 0 {
		return nil, fmt.Errorf("failed to schedule peers")
	}

	result := make([]*Result, len(pns))
	for i, pn := range pns {
		node, err := sm.nodeContainer.getAsNode(pn.peerID)
		if err != nil {
			logrus.Errorf("failed to get node: %v", err)
			continue
		}

		result[i] = &Result{
			DstCid: pn.peerID,
			Pieces: pn.pieces,
			Task:  pn.info,
			PeerInfo: node.Basic,
			Generation: sm.generation,
			StartDownload: func(peerID string, generation int64) {
				sm.downloadStartCh <- notifySt{peerID: peerID, generation: generation}
			},
			FinishDownload: func(peerID string, generation int64) {
				sm.downloadFinishCh <- notifySt{peerID: peerID, generation: generation}
			},
		}
	}

	return result, nil
}

func (sm *SchedulerManager) SchedulerByUrl(ctx context.Context, url string, srcCid string, pieceRange string, pieceSize int32) ([]*Result, error) {
	return nil, nil
}

func (sm *SchedulerManager) SyncSchedulerInfo(nodes []*types.Node) {
	newTaskContainer := newDataMap()
	newNodeContainer := newDataMap()
	// todo: urlContainer init

	for _, node := range nodes {
		newNodeContainer.add(node.Basic.ID, node)
		sm.syncTaskContainerPerNode(node, newTaskContainer)
	}

	// replace the taskContainer and nodeContainer
	sm.Lock()
	defer sm.Unlock()

	sm.taskContainer = newTaskContainer
	sm.nodeContainer = newNodeContainer
}

func (sm *SchedulerManager) syncTaskContainerPerNode(node *types.Node, taskContainer *dataMap) {
	for _, task := range node.Tasks {
		ts, err := taskContainer.getAsTaskState(task.Task.ID)
		if err != nil && !errortypes.IsDataNotFound(err) {
			logrus.Errorf("syncTaskContainerPerNode error: %v", err)
			continue
		}

		if ts == nil {
			ts = newTaskState()
			if err := taskContainer.add(task.Task.ID, ts); err != nil {
				logrus.Errorf("syncTaskContainerPerNode add taskstate %v to taskContainer error: %v", ts, err)
				continue
			}
		}

		err = ts.add(node.Basic.ID, &node.Load, task.Pieces, task.Task)
		if err != nil {
			logrus.Errorf("syncTaskContainerPerNode error: %v", err)
		}
	}
}

func (sm *SchedulerManager) updateNodeLoad(peerID string, addLoad int) {
	sm.Lock()
	defer sm.Unlock()

	node, err := sm.nodeContainer.getAsNode(peerID)
	if err != nil {
		logrus.Errorf("updateNodeLoad failed: %v", err)
		return
	}

	node.Load = node.Load + int64(addLoad)
	if node.Load < 0 {
		node.Load = 0
	}
}
