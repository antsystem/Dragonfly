package scheduler

import (
	"context"
	"sync"

	"github.com/dragonflyoss/Dragonfly/apis/types"
	"github.com/dragonflyoss/Dragonfly/pkg/errortypes"

	"github.com/sirupsen/logrus"
)

const (
	defaultMaxPeers = 3
)

// Manager schedules the peer node to fetch the resource specified by request
type Manager struct {
	mutex         sync.Mutex
	localPeerInfo *types.PeerInfo
	// generation
	generation int64

	// key is peerID, value is Node
	nodeContainer *dataMap

	// key is the url
	urlContainer *dataMap

	// key is url, value is taskState
	seedContainer *dataMap

	// key is url, value is localTaskState
	localSeedContainer *dataMap
}

func NewScheduler(localPeer *types.PeerInfo) *Manager {
	sm := &Manager{
		localSeedContainer: newDataMap(),
		nodeContainer:      newDataMap(),
		urlContainer:       newDataMap(),
		seedContainer:      newDataMap(),
		localPeerInfo:      localPeer,
	}

	return sm
}

// if pieceRange == "" means all Pieces of file
func (sm *Manager) Scheduler(ctx context.Context, url string) []*Result {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	result := []*Result{}

	// get local seed
	localRs := sm.scheduleLocalPeer(url)
	if len(localRs) > 0 {
		result = append(result, localRs...)
	}

	remoteRs := sm.scheduleRemotePeer(ctx, url)
	if len(remoteRs) > 0 {
		result = append(result, remoteRs...)
	}

	return result
}

func (sm *Manager) scheduleRemotePeer(ctx context.Context, url string) []*Result {
	var (
		state *taskState
		err   error
	)

	state, err = sm.seedContainer.getAsTaskState(url)
	if err != nil {
		return nil
	}

	pns := state.getPeersByLoad(defaultMaxPeers)
	if len(pns) == 0 {
		return nil
	}

	result := make([]*Result, len(pns))
	for i, pn := range pns {
		node, err := sm.nodeContainer.getAsNode(pn.peerID)
		if err != nil {
			logrus.Errorf("failed to get node: %v", err)
			continue
		}

		result[i] = &Result{
			DstCid:   pn.peerID,
			Path:     pn.path,
			Task:     pn.info,
			PeerInfo: node.Basic,
		}
	}

	return result
}

func (sm *Manager) SyncSchedulerInfo(nodes []*types.Node) {
	newNodeContainer := newDataMap()
	seedContainer := newDataMap()
	// todo: urlContainer init

	for _, node := range nodes {
		newNodeContainer.add(node.Basic.ID, node)
		sm.syncSeedContainerPerNode(node, seedContainer)
	}

	// replace the taskContainer and nodeContainer
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	sm.nodeContainer = newNodeContainer
	sm.seedContainer = seedContainer
}

func (sm *Manager) AddLocalSeedInfo(task *types.TaskFetchInfo) {
	sm.localSeedContainer.add(task.Task.TaskURL, &localTaskState{task: task})
}

func (sm *Manager) DeleteLocalSeedInfo(url string) {
	sm.localSeedContainer.remove(url)
}

func (sm *Manager) syncSeedContainerPerNode(node *types.Node, seedContainer *dataMap) {
	for _, task := range node.Tasks {
		if !task.Task.AsSeed {
			continue
		}

		ts, err := seedContainer.getAsTaskState(task.Task.TaskURL)
		if err != nil && !errortypes.IsDataNotFound(err) {
			logrus.Errorf("syncSeedContainerPerNode error: %v", err)
			continue
		}

		if ts == nil {
			ts = newTaskState()
			if err := seedContainer.add(task.Task.TaskURL, ts); err != nil {
				logrus.Errorf("syncSeedContainerPerNode add taskstate %v to taskContainer error: %v", ts, err)
				continue
			}
		}

		err = ts.add(node.Basic.ID, task.Pieces[0].Path, task.Task)
		if err != nil {
			logrus.Errorf("syncSeedContainerPerNode error: %v", err)
		}
	}
}

func (sm *Manager) scheduleLocalPeer(url string) []*Result {
	var (
		lts *localTaskState
		err error
	)

	// seed file has the priority
	lts, err = sm.localSeedContainer.getAsLocalTaskState(url)
	if err == nil {
		return []*Result{sm.covertLocalTaskStateToResult(lts)}
	}

	return nil
}

func (sm *Manager) covertLocalTaskStateToResult(lts *localTaskState) *Result {
	return &Result{
		DstCid:   sm.localPeerInfo.ID,
		PeerInfo: sm.localPeerInfo,
		Task:     lts.task.Task,
		Path:     lts.path,
		Local:    true,
	}
}
