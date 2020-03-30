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
	localPeerInfo  *types.PeerInfo
	// generation
	generation     int64

	//// key is taskID, value is taskState
	//taskContainer  *dataMap
	//
	//// key is taskID, value is localTaskState
	//localTaskContainer *dataMap

	// key is peerID, value is Node
	nodeContainer  *dataMap

	// key is the url
	urlContainer  *dataMap

	// key is url, value is taskState
	seedContainer *dataMap

	// key is url, value is localTaskState
	localSeedContainer *dataMap

	downloadStartCh  chan notifySt
	downloadFinishCh chan notifySt
}

func NewScheduler(localPeer *types.PeerInfo) *SchedulerManager {
	sm := &SchedulerManager{
		//taskContainer: newDataMap(),
		//localTaskContainer: newDataMap(),
		localSeedContainer: newDataMap(),
		nodeContainer: newDataMap(),
		urlContainer: newDataMap(),
		seedContainer: newDataMap(),
		localPeerInfo: localPeer,
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
func (sm *SchedulerManager) Scheduler(ctx context.Context, url string, pieceRange string, pieceSize int32) ([]*Result, error) {
	sm.Lock()
	defer sm.Unlock()

	result := []*Result{}

	// get local task if
	localRs := sm.scheduleLocalPeer(url)
	if len(localRs) > 0 {
		result = append(result, localRs...)
	}

	remoteRs, err := sm.scheduleRemotePeer(ctx, url, pieceRange, pieceSize)
	if err == nil && len(remoteRs) > 0 {
		result = append(result, remoteRs...)
	}

	return result, nil
}

func (sm *SchedulerManager) scheduleRemotePeer(ctx context.Context, url string, pieceRange string, pieceSize int32) ([]*Result, error)  {
	var(
		state *taskState
		err error
	)

	if url != "" {
		state, err = sm.seedContainer.getAsTaskState(url)
		if err == nil {
			goto next
		}
	}

next:
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
			Path: pn.path,
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

func (sm *SchedulerManager) SyncSchedulerInfo(nodes []*types.Node) {
	newTaskContainer := newDataMap()
	newNodeContainer := newDataMap()
	seedContainer := newDataMap()
	// todo: urlContainer init

	for _, node := range nodes {
		newNodeContainer.add(node.Basic.ID, node)
		sm.syncTaskContainerPerNode(node, newTaskContainer, seedContainer)
	}

	// replace the taskContainer and nodeContainer
	sm.Lock()
	defer sm.Unlock()

	sm.nodeContainer = newNodeContainer
	sm.seedContainer = seedContainer
}

func (sm *SchedulerManager) AddLocalSeedInfo(task *types.TaskFetchInfo) {
	sm.localSeedContainer.add(task.Task.TaskURL, &localTaskState{task: task})
}

func (sm *SchedulerManager) DeleteLocalSeedInfo(url string) {
	sm.localSeedContainer.remove(url)
}

func (sm *SchedulerManager) syncTaskContainerPerNode(node *types.Node, taskContainer *dataMap, seedContainer *dataMap) {
	for _, task := range node.Tasks {
		if !task.Task.AsSeed {
			continue
		}

		ts, err := seedContainer.getAsTaskState(task.Task.TaskURL)
		if err != nil && !errortypes.IsDataNotFound(err) {
			logrus.Errorf("syncTaskContainerPerNode error: %v", err)
			continue
		}

		if ts == nil {
			ts = newTaskState()
			if err := seedContainer.add(task.Task.TaskURL, ts); err != nil {
				logrus.Errorf("syncTaskContainerPerNode add taskstate %v to taskContainer error: %v", ts, err)
				continue
			}
		}

		err = ts.add(node.Basic.ID, &node.Load, task.Pieces[0].Path, task.Task)
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

func (sm *SchedulerManager) scheduleLocalPeer(url string) []*Result {
	var(
		lts *localTaskState
		err error
	)

	result := []*Result{}

	if url != "" {
		// seed file has the priority
		lts, err = sm.localSeedContainer.getAsLocalTaskState(url)
		if err == nil {
			result = append(result, sm.covertLocalTaskStateToResult(lts))
		}
	}

	return result
}

func (sm *SchedulerManager) covertLocalTaskStateToResult(lts *localTaskState) *Result {
	return &Result{
		DstCid: sm.localPeerInfo.ID,
		PeerInfo: sm.localPeerInfo,
		Task: lts.task.Task,
		Path: lts.path,
		Generation: sm.generation,
		Local: true,
	}
}
