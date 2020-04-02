package scheduler

import (
	"github.com/dragonflyoss/Dragonfly/apis/types"
)

type Result struct {
	DstCid   string
	Local    bool
	PeerInfo *types.PeerInfo
	Task     *types.TaskInfo
	Path     string
}

type notifySt struct {
	peerID     string
	generation int64
}

type localTaskState struct {
	task *types.TaskFetchInfo
	path string
}
